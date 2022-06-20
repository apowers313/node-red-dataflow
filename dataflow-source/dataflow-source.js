const {DataflowSource, DataflowSink, DataflowThrough} = require("dataflow-core");
const {WritableStreamDefaultController, ReadableStreamDefaultController} = require("node:stream/web");

function printStack() {
    const foo = {};
    Error.captureStackTrace(foo);
    console.log("stack", foo.stack);
    throw new Error("done");
}

function getNextNodes(RED, node) {
    // TODO: RED.nodes.getNode would be more optimal, but it always seems to return 'null'
    // console.log("node._flow", node._flow);
    const results = [];
    const wires = node.wires.flat();
    wires.forEach((w) => {
        console.log("getting node:", w);
        let n = node._flow.getNode(w);
        results.push(n);
    });
    // RED.nodes.eachNode(function(n) {
    //     if (wires.includes(n.id)) {
    //         results.push(n);
    //     }
    // });

    return results;
}

function isDataflowNode(node) {
    // TODO: smarter test
    if (node.type !== "dataflow-source" && node.type !== "dataflow-sink") {
        return false;
    }

    return true;
}

function isSinkNode(node) {
    if (node.wires.length === 0) {
        return true;
    }

    return false;
}

function passthroughFn(node, inputCallbacks, msg, controller) {
    console.log("calling passthroughFn");
    console.log("node", node);
    console.log("inputCallbacks", inputCallbacks);
    console.log("msg", msg);
    console.log("controller", controller);

    let send;
    let done = (err) => {
        if (err) {
            throw err;
        }
    };
    if (controller instanceof WritableStreamDefaultController) {
        send = () => {
            throw new Error("why the heck is your sink sending data?");
        };
    } else if (controller instanceof ReadableStreamDefaultController) {
        send = (... args) => {
            controller.enqueue.bind(controller, args);
        };
    } else {
        throw new TypeError("expected controller to be a ReadableStreamDefaultController or WritableStreamDefaultController");
    }

    node.send = send;
    inputCallbacks.forEach((listener) => {
        console.log("calling listener with", msg);
        listener.call(node, msg, send, done);
    });
}

function coerceNode(RED, node) {
    console.log("coercing node:", node);

    // XXX: Node-RED's definition of Node inherits EventEmitter, but then overloads ".on" and stores inputCallbacks separately
    let inputCallbacks = [node._inputCallback];
    node._inputCallback = null;
    if (node._inputCallbacks) {
        inputCallbacks = node._inputCallbacks;
        node._inputCallbacks = null;
    }

    let dfConstructor;
    let dfConfig;
    if (isSinkNode(node)) {
        console.log("making new node a sink");
        dfConstructor = DataflowSink;
        dfConfig = {push: passthroughFn.bind(null, node, inputCallbacks)};
    } else {
        console.log("making new node a through");
        dfConstructor = DataflowThrough;
        dfConfig = {through: passthroughFn.bind(null, node, inputCallbacks)};
    }

    dataflowAttachToNode(RED, node, dfConstructor, dfConfig);
}

function coerceNextNodes(RED, node) {
    const nodes = getNextNodes(RED, node);

    nodes.forEach((n) => {
        if (!isDataflowNode(n)) {
            coerceNode(RED, n);
        }
    });
}

function dataflowAttachToNode(RED, node, dfConstructor, dfConfig) {
    console.log("attaching to node", node);

    node.on("input", (msg, send) => {
        // create a new pipe everytime the flow is run
        const df = new dfConstructor(dfConfig);
        coerceNextNodes(RED, node);

        // if this is a DataflowSource we ignore the input message
        if (!(df instanceof DataflowSource)) {
            // treat the input mesage as a readable stream
            if (!(msg instanceof DataflowSource) && !(msg instanceof DataflowThrough)) {
                throw new TypeError("dataflowEventMonkeypatch expected to receive a DataflowComponent as input");
            }

            msg.pipe(df);
        } else {
            console.log("df instanceof DataflowSource", df instanceof DataflowSource);
            // if this is a source, wait for the stream to complete
            df.complete()
                .then(() => {
                    console.log("source is complete");
                })
                .catch((err) => {
                    console.log("ERROR");
                    console.error(err);
                    node.error(err);
                });
        }

        send(df);
    });
}

module.exports = function(RED) {
    console.log("registering Source");
    function Source(config) {
        console.log("constructing Source");

        RED.nodes.createNode(this, config);
        const {EventEmitter} = require("events");
        console.log("Source is event emitter", this instanceof EventEmitter);

        const node = this;

        let count = 0;
        const sourceConfig = {
            pull() {
                if (count > 10) {
                    console.log("SOURCE: done.");
                    return null;
                }

                console.log("SOURCE: sending count", count);
                return {count: count++};
            },
        };

        // TODO: if there's an upstream node wait for input... if there's not just start now

        dataflowAttachToNode(RED, node, DataflowSource, sourceConfig);
    }
    RED.nodes.registerType("dataflow-source", Source);
};
