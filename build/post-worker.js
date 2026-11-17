  return __ffmpegjs_return;
}

// ---------------------------------------------------------------------------
// Recoverable worker state machine.
//
// States: "idle" -> "running" -> "idle".
//
// Every termination path of a job (normal return, FFmpeg error exit, hard
// abort, cancellation) funnels through the same cleanup which resets the
// machine to "idle", so a sudden failure can never wedge the worker and the
// same Worker can safely accept subsequent jobs. Each job runs with a fresh
// Module/MEMFS instance; partial outputs of failed or cancelled jobs are
// discarded and never leak into the results of later jobs.
// ---------------------------------------------------------------------------

var __ffmpegjs_state = "idle";
var __ffmpegjs_cancel_requested = false;

// Exit code reported for cancelled jobs (128 + SIGINT), mirroring shells.
var __FFMPEGJS_EXIT_CANCELLED = 130;

// Shim for nodejs
if (typeof self === "undefined") {
  self = require("worker_threads")["parentPort"];
}

// Emscripten unwinds the stack after exit() by throwing ExitStatus; this is
// a normal termination, not a crash.
function __ffmpegjs_isExitStatus(err) {
  return !!err &&
    (err.name === "ExitStatus" ||
     (typeof err.status === "number" && /exited/i.test(String(err.message))));
}

self.onmessage = function(e) {
  var msg = e.data;
  if (msg["type"] == "run") {
    if (__ffmpegjs_state !== "idle") {
      self.postMessage({"type": "error", "data": "already running"});
      return;
    }
    __ffmpegjs_state = "running";
    __ffmpegjs_cancel_requested = false;
    self.postMessage({"type": "run"});

    // Cooperative cancellation. The host may either pass an
    // Int32Array-backed SharedArrayBuffer as `cancelBuffer` (works while
    // the job is running, even though the event loop is blocked) or send
    // a {"type": "cancel"} message (takes effect at the next output
    // callback). The flag is polled on every stdout/stderr line, which
    // FFmpeg emits regularly during transcoding.
    var cancelFlag = null;
    if (msg["cancelBuffer"]) {
      try {
        cancelFlag = new Int32Array(msg["cancelBuffer"]);
      } catch (err) {
        cancelFlag = null;
      }
    }
    function __ffmpegjs_checkCancel() {
      if (__ffmpegjs_cancel_requested || (cancelFlag && cancelFlag[0])) {
        var err = new Error("cancelled");
        err.__ffmpegjs_cancelled = true;
        throw err;
      }
    }

    var opts = {};
    Object.keys(msg).forEach(function(key) {
      if (key !== "type" && key !== "cancelBuffer") {
        opts[key] = msg[key]
      }
    });
    opts["print"] = function(line) {
      __ffmpegjs_checkCancel();
      self.postMessage({"type": "stdout", "data": line});
    };
    opts["printErr"] = function(line) {
      __ffmpegjs_checkCancel();
      self.postMessage({"type": "stderr", "data": line});
    };
    var exitCode = null;
    opts["onExit"] = function(code) {
      exitCode = code;
      self.postMessage({"type": "exit", "data": code});
    };
    var abortPosted = false;
    opts["onAbort"] = function(reason) {
      abortPosted = true;
      self.postMessage({"type": "abort", "data": reason});
    };

    var result = null;
    var failure = null;
    try {
      var runResult = __ffmpegjs(opts);
      if (runResult && runResult["MEMFS"]) {
        result = runResult;
      }
    } catch (err) {
      failure = err;
    }

    if (failure && failure.__ffmpegjs_cancelled) {
      // Cancelled: discard all partial output, notify and recover.
      self.postMessage({"type": "cancelled"});
      if (exitCode === null) {
        self.postMessage({"type": "exit", "data": __FFMPEGJS_EXIT_CANCELLED});
      }
    } else if (failure && !__ffmpegjs_isExitStatus(failure)) {
      // Hard failure (wasm trap, abort(), uncaught exception): report and
      // recover. No partial MEMFS state escapes to the client.
      if (!abortPosted) {
        self.postMessage({"type": "abort",
          "data": String(failure && failure.message || failure)});
      }
      if (exitCode === null) {
        self.postMessage({"type": "exit", "data": 1});
      }
    } else {
      // Normal return or clean ExitStatus unwind. On error exits postRun
      // may not have run, so never leak partial or stale MEMFS contents.
      if (!result) {
        result = {"MEMFS": []};
      }
      var transfer = result["MEMFS"].map(function(file) {
        return file["data"].buffer;
      });
      self.postMessage({"type": "done", "data": result}, transfer);
    }

    // Recover: the worker is ready for the next job no matter how this
    // one ended.
    __ffmpegjs_state = "idle";
    __ffmpegjs_cancel_requested = false;
  } else if (msg["type"] == "cancel") {
    // Request cancellation of the current job. Note that while the
    // asm.js/wasm core is executing, the event loop is blocked and this
    // message is only delivered afterwards; use the `cancelBuffer`
    // SharedArrayBuffer option of "run" for immediate cancellation.
    if (__ffmpegjs_state === "running") {
      __ffmpegjs_cancel_requested = true;
    }
  } else {
    self.postMessage({"type": "error", "data": "unknown command"});
  }
};

self.postMessage({"type": "ready"});
