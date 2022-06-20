const {DataflowSource, DataflowThrough, DataflowSink} = require("dataflow-core");

module.exports = function(RED) {
    console.log("registering Sink");
    function Sink(config) {
        console.log("constructing Sink");
        RED.nodes.createNode(this, config);

        const node = this;

        node.on("input", function(src) {
            if (!(src instanceof DataflowSource) && !(src instanceof DataflowThrough)) {
                throw new TypeError("expected msg to be an instance of DataflowSource or DataflowThrough");
            }

            const dst = new DataflowSink({
                push(msg) {
                    node.warn(msg);
                },
            });

            src.pipe(dst);
        });
    }
    RED.nodes.registerType("dataflow-sink", Sink);
};
