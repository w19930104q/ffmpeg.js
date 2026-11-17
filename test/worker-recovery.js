// Recovery test for the ffmpeg.js worker state machine (build/post-worker.js).
//
// The real ffmpeg-worker-*.js builds require Emscripten, so this test wraps
// the *actual* post-worker.js state machine around a mock ffmpeg core that
// reproduces the three relevant termination behaviors of the Emscripten
// runtime:
//
//   1. error exit   - onExit(1) followed by an ExitStatus throw
//   2. long job     - prints progress until cooperatively cancelled
//   3. success      - onExit(0) and a MEMFS result
//
// The same Worker runs all three jobs sequentially (fail -> cancel ->
// success) proving that the worker recovers after sudden termination and
// that MEMFS outputs stay isolated between jobs. Prints the exit code and
// artifact hashes of every step.
//
// Run: node test/worker-recovery.js

"use strict";

var fs = require("fs");
var os = require("os");
var path = require("path");
var crypto = require("crypto");
var Worker = require("worker_threads").Worker;

var POST_WORKER = path.join(__dirname, "..", "build", "post-worker.js");

var MOCK_CORE = `
function ExitStatus(status) {
  this.name = "ExitStatus";
  this.message = "Program has exited with status " + status;
  this.status = status;
}
function __ffmpegjs(opts) {
  var args = opts["arguments"] || [];
  if (args.indexOf("--fail") >= 0) {
    // Emulates an FFmpeg error exit: quit(1) then ExitStatus unwind.
    opts["printErr"]("mock: invalid stream, transcoding failed");
    if (opts["onExit"]) opts["onExit"](1);
    throw new ExitStatus(1);
  }
  if (args.indexOf("--long") >= 0) {
    // Emulates a long transcode: prints progress lines until the
    // printErr shim (post-worker.js) throws on cancellation.
    var start = Date.now();
    var frame = 0;
    while (Date.now() - start < 60000) {
      opts["printErr"]("frame= " + (++frame) + " fps= 25");
      var t = Date.now();
      while (Date.now() - t < 20) {}
    }
    if (opts["onExit"]) opts["onExit"](0);
    return {"MEMFS": [{"name": "long.webm", "data": new Uint8Array(1)}]};
  }
  // Success: deterministic output derived from this job's input only.
  var input = (opts["MEMFS"] || [])[0];
  var n = input ? input["data"].length : 0;
  var data = new Uint8Array(64);
  for (var i = 0; i < data.length; i++) data[i] = (n + i) & 0xff;
  opts["print"]("mock: transcoding done");
  if (opts["onExit"]) opts["onExit"](0);
  return {"MEMFS": [{"name": "out.webm", "data": data}]};
}
`;

function sha256(data) {
  return crypto.createHash("sha256").update(Buffer.from(data)).digest("hex");
}

function buildMockWorker() {
  // post-worker.js is a build fragment: its first two lines close the
  // __ffmpegjs function opened by pre.js. Strip them for the mock build.
  var post = fs.readFileSync(POST_WORKER, "utf8");
  var stripped = post.replace(/^[^\n]*return __ffmpegjs_return;[^\n]*\n\}/, "");
  if (stripped === post) {
    throw new Error("could not strip __ffmpegjs prologue from post-worker.js");
  }
  var file = path.join(
    fs.mkdtempSync(path.join(os.tmpdir(), "ffmpegjs-recovery-")),
    "mock-ffmpeg-worker.js");
  fs.writeFileSync(file, MOCK_CORE + "\n" + stripped);
  return file;
}

// Runs one job on the worker and resolves with the collected message log.
function runJob(worker, message, onMessage) {
  return new Promise(function(resolve, reject) {
    var log = {exit: null, done: null, cancelled: false, abort: null, error: null};
    function handler(msg) {
      switch (msg.type) {
      case "exit":
        log.exit = msg.data;
        break;
      case "done":
        log.done = msg.data;
        return cleanup(function() { resolve(log); });
      case "cancelled":
        log.cancelled = true;
        break;
      case "abort":
        log.abort = msg.data;
        break;
      case "error":
        log.error = msg.data;
        return cleanup(function() {
          reject(new Error("worker error: " + msg.data));
        });
      }
      // A cancelled job ends with "cancelled" + "exit" and no "done".
      if (log.cancelled && log.exit !== null) {
        return cleanup(function() { resolve(log); });
      }
      if (onMessage) onMessage(msg);
    }
    function cleanup(cb) {
      worker.removeListener("message", handler);
      cb();
    }
    worker.on("message", handler);
    worker.postMessage(message);
  });
}

var failures = 0;
function check(name, cond) {
  if (cond) {
    console.log("  ok - " + name);
  } else {
    failures++;
    console.error("  FAIL - " + name);
  }
}

function report(step, log) {
  var hashes = log.done && log.done.MEMFS.length
    ? log.done.MEMFS.map(function(f) {
        return f.name + " sha256=" + sha256(f.data);
      }).join(", ")
    : "(no artifacts)";
  console.log("[step " + step + "] exit=" + log.exit +
    (log.cancelled ? " cancelled" : "") +
    (log.abort ? " abort=" + JSON.stringify(log.abort) : "") +
    " artifacts: " + hashes);
}

async function main() {
  var workerFile = buildMockWorker();
  var worker = new Worker(workerFile);
  worker.on("error", function(err) {
    console.error("worker crashed:", err);
    process.exit(1);
  });
  await new Promise(function(resolve) {
    worker.on("message", function ready(msg) {
      if (msg.type === "ready") {
        worker.removeListener("message", ready);
        resolve();
      }
    });
  });

  // Step 1: failing job. Must not wedge the worker.
  console.log("step 1: failing transcode");
  var fail = await runJob(worker, {type: "run", arguments: ["--fail"]});
  report(1, fail);
  check("exit code 1", fail.exit === 1);
  check("done received after failure", !!fail.done);
  check("no artifacts leak from failed job",
    fail.done && fail.done.MEMFS.length === 0);

  // Step 2: long job cancelled mid-run via SharedArrayBuffer flag.
  console.log("step 2: cancel running transcode");
  var cancelFlag = new Int32Array(new SharedArrayBuffer(4));
  var sawProgress = false;
  var cancel = await runJob(worker, {
    type: "run",
    arguments: ["--long"],
    cancelBuffer: cancelFlag.buffer,
  }, function onMessage(msg) {
    if (msg.type === "stderr" && !sawProgress) {
      sawProgress = true;
      cancelFlag[0] = 1; // cancel while the worker thread is busy
    }
  });
  report(2, cancel);
  check("job was cancelled", cancel.cancelled);
  check("exit code 130 (128+SIGINT)", cancel.exit === 130);
  check("no done message for cancelled job", !cancel.done);

  // Step 3: successful job on the same worker.
  console.log("step 3: successful transcode after fail+cancel");
  var ok = await runJob(worker, {
    type: "run",
    arguments: [],
    MEMFS: [{name: "in.webm", data: new Uint8Array(100)}],
  });
  report(3, ok);
  check("exit code 0", ok.exit === 0);
  check("exactly one artifact", ok.done && ok.done.MEMFS.length === 1);
  check("artifact is out.webm",
    ok.done && ok.done.MEMFS[0] && ok.done.MEMFS[0].name === "out.webm");

  // Isolation: the successful artifact must be reproducible and untouched
  // by the previous jobs' state.
  var ok2 = await runJob(worker, {
    type: "run",
    arguments: [],
    MEMFS: [{name: "in.webm", data: new Uint8Array(100)}],
  });
  var h1 = sha256(ok.done.MEMFS[0].data);
  var h2 = sha256(ok2.done.MEMFS[0].data);
  console.log("[isolation] rerun sha256=" + h2);
  check("identical input gives identical hash across jobs", h1 === h2);

  await worker.terminate();
  fs.unlinkSync(workerFile);
  fs.rmdirSync(path.dirname(workerFile));
  if (failures) {
    console.error(failures + " check(s) failed");
    process.exit(1);
  }
  console.log("all checks passed: worker recovered after fail, cancel, success");
}

main().catch(function(err) {
  console.error(err);
  process.exit(1);
});
