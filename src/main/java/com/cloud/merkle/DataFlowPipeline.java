package com.cloud.merkle;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.stream.StreamSupport;

public class DataFlowPipeline {
    static Logger logger = LoggerFactory.getLogger(DataFlowPipeline.class);

    public static class Node implements Serializable {
        int seq;
        byte[] data;

        int getSeq() {return seq;}
        byte[] getData() {return data;}
    }

    public static class FileToByteFn extends DoFn<String, Node> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Node res = new Node();
            String line = c.element();
            res.seq = Integer.parseInt(line.split(",")[0]);
            res.data = line.getBytes(StandardCharsets.UTF_8);
            c.output(res);
        }
    }

    public static class HashRawFn extends DoFn<Node, Node> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                Node hashed = new Node();
                hashed.data = md.digest(c.element().data);
                c.output(hashed);
            } catch (NoSuchAlgorithmException e) {

            }
        }
    }

    public static class ToKV extends DoFn<Node, KV<Integer, Node>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Node node = c.element();
            c.output(KV.of(node.seq / 1024, node));
        }
    }

    public static class ToMerkle extends DoFn<KV<Integer, Iterable<Node>>, Node> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            byte[][] data = StreamSupport.stream(c.element().getValue().spliterator(), false)
                    .sorted(Comparator.comparingInt(Node::getSeq))
                    .map(Node::getData)
                    .toArray(byte[][]::new);
            try {
                byte[] root = RecursiveMerkleTree.generateMerkleRoot(data);
                Node res = new Node();
                res.seq = c.element().getKey();
                res.data = root;
                c.output(res);
            } catch (NoSuchAlgorithmException e) {

            }
        }
    }

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create(PipelineOptionsFactory.fromArgs(args).create());

        PCollection<String> lines = pipeline
                .apply("Read File", TextIO.read()
                        .from("gs://run-sources-protean-music-381914-us-central1/data/standard/test?.txt")
                );
        PCollection<Node> byteLines = lines.apply("To Byte", ParDo.of(new FileToByteFn()));
        PCollection<Node> hashLines = byteLines.apply("Hash Raw", ParDo.of(new HashRawFn()));
        PCollection<KV<Integer, Node>> kvNodes = hashLines.apply("To KV", ParDo.of(new ToKV()));
        PCollection<KV<Integer, Iterable<Node>>> chunks = kvNodes.apply("Group Chunk", GroupByKey.create());
        PCollection<KV<Integer, Node>> mediators = chunks.apply("To Merkle", ParDo.of(new ToMerkle()))
                .apply("To KV", ParDo.of(new ToKV()));
        PCollection<KV<Integer, Iterable<Node>>> lastTree = mediators.apply("To Merkle Root", GroupByKey.create());
        PCollection<Node> root = lastTree.apply("To Root", ParDo.of(new ToMerkle()));
        root.apply("Log", ParDo.of(new DoFn<Node, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                logger.warn("DTran complete");
            }
        }));
        pipeline.run().waitUntilFinish();
    }
}
