package org.curiousjaki.zkvmambassador;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobWorker;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import poam.Poam;
import poam.VerifiableProcessingServiceGrpc;

import java.time.Duration;
import java.util.*;



//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    private static class ProveJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {

            // here: business logic that is executed with every job
            Poam.ProveResponse response = prove(job);
            String public_outout = response.getPublicOutput();
            Poam.Proof proofResult = response.getProofResponse();
            // Create output variables
            Map<String, Object> variables = job.getVariablesAsMap();
            variables.put("public_output", public_outout);

            if (!variables.containsKey("proof_chain")) {
                variables.put("proof_chain", new ArrayList<String>());
            }

            try {
                ((List<String>) variables.get("proof_chain")).add(JsonFormat.printer().print(response.getProofResponse()));
                variables.put("previous_proof", JsonFormat.printer().print(response.getProofResponse()));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }

            // Complete job with new variables
            client.newCompleteCommand(job.getKey())
                    .variables(variables)
                    .send()
                    .join();
        }
    }
    private static class ComposeJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {

            // here: business logic that is executed with every job
            Poam.CompositionResponse response = compose(job);
            //String public_outout = response.getPublicOutput();
            Poam.Proof proofResult = response.getProofResponse();
            // Create output variables
            Map<String, Object> variables = job.getVariablesAsMap();
            //variables.put("public_output", public_outout);

            if (!variables.containsKey("proof_chain")) {
                variables.put("proof_chain", new ArrayList<String>());
            }

            try {
                ((List<String>) variables.get("proof_chain")).add(JsonFormat.printer().print(response.getProofResponse()));
                variables.put("previous_proof", JsonFormat.printer().print(response.getProofResponse()));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }

            // Complete job with new variables
            client.newCompleteCommand(job.getKey())
                    .variables(variables)
                    .send()
                    .join();
        }
    }
    private static class VerifyJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {

            // here: business logic that is executed with every job
            Poam.VerifyResponse response = verify(job);
            Map<String, Object> variables = new HashMap<>();
            variables.put("verification_output", response.getPublicOutput());
            variables.put("is_valid_executed", response.getIsValidExecuted());

            // Complete job with new variables
            client.newCompleteCommand(job.getKey())
                    .variables(variables)
                    .send()
                    .join();
        }
    }
    private static class CombineJobHandler implements JobHandler {
        @Override
        public void handle(final JobClient client, final ActivatedJob job) {

            // here: business logic that is executed with every job
            Poam.ProveResponse response = single_step(job);
            String public_outout = response.getPublicOutput();
            Poam.Proof proofResult = response.getProofResponse();
            // Create output variables
            Map<String, Object> variables = new HashMap<>();
            variables.put("public_output", public_outout);
            try {
                variables.put("previous_proof", JsonFormat.printer().print(response.getProofResponse()));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }

            // Complete job with new variables
            client.newCompleteCommand(job.getKey())
                    .variables(variables)
                    .send()
                    .join();
        }
    }

    public static void main(String[] args) {

        ZeebeClient client = ZeebeClient.newClientBuilder()
                .gatewayAddress("localhost:26500")
                .usePlaintext()
                .defaultJobWorkerMaxJobsActive(1)
                .build();

        client.newTopologyRequest().send().join();

        System.out.println("Opening job worker.");

        try (
                final JobWorker provingWorker = client
                             .newWorker()
                             .jobType("proving-job")
                             .handler(new ProveJobHandler())
                             .timeout(Duration.ofSeconds(1000))
                             .open();
                final JobWorker verificationWorker = client
                            .newWorker()
                            .jobType("verify-job")
                            .handler(new VerifyJobHandler())
                            .open();
                final JobWorker combinedWorker = client
                        .newWorker()
                        .jobType("combine-job")
                        .handler(new CombineJobHandler())
                        .open();
                final JobWorker composeWorker = client
                        .newWorker()
                        .jobType("compose-job")
                        .handler(new ComposeJobHandler())
                        .open()
        ) {
            System.out.println("Job workers opened and receiving jobs.");

            // run until System.in receives exit command
            waitUntilSystemInput("exit");
        }

    }
    private static void waitUntilSystemInput(final String exitCode) {
        try (final Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                final String nextLine = scanner.nextLine();
                if (nextLine.contains(exitCode)) {
                    return;
                }
            }
        }
    }

    public static Poam.VerifyResponse verify(ActivatedJob activatedJob){
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()  // Only for testing; don't use in production
                .build();
        VerifiableProcessingServiceGrpc.VerifiableProcessingServiceBlockingStub stub =
                VerifiableProcessingServiceGrpc.newBlockingStub(channel);

        Map<String, Object> variables = activatedJob.getVariablesAsMap();
        Poam.VerifyRequest request;
        if (variables.containsKey("previous_proof")) {
            String previous_json = variables.get("previous_proof").toString();
            Poam.Proof.Builder builder = Poam.Proof.newBuilder();
            try {
                JsonFormat.parser().merge(previous_json, builder);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            Poam.Proof parsed = builder.build();
            request = Poam.VerifyRequest.newBuilder()
                    .setProof(parsed)
                    .build();
        } else {
            System.out.println("Variable 'previous_public_output' is not available.");
            request = Poam.VerifyRequest.newBuilder()
                    .build();
        }
        // Make the call and receive the response
        Poam.VerifyResponse response = stub.verify(request);

        // Output the result
        System.out.println("Proving result: " + response.getIsValidExecuted());

        // Shut down the channel
        channel.shutdown();
        return response;
    }

    public static Poam.ProveResponse prove(ActivatedJob activatedJob){
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()  // Only for testing; don't use in production
                .build();
        VerifiableProcessingServiceGrpc.VerifiableProcessingServiceBlockingStub stub =
                VerifiableProcessingServiceGrpc.newBlockingStub(channel);
        List<Integer> imageId = List.of((int) 1910843796L, (int) 268995076L, (int) 638532809L, (int) 3011961156L,(int) 1301210768L,(int) 2805976851L,(int) 2467108861L, (int)1954251535L);
        //System.out.println(imageId);
        String methodPayload = String.format("{\"a\":%s,\"b\":%s,\"operation\":\"%s\"}",
                activatedJob.getVariable("a").toString(),
                activatedJob.getVariable("b").toString(),
                activatedJob.getVariable("operation").toString());

        Map<String, Object> variables = activatedJob.getVariablesAsMap();
        Poam.ProveRequest request;
        if (variables.containsKey("composition") && (boolean) variables.get("composition")) {
            String previous_json = variables.get("previous_proof").toString();
            Poam.Proof.Builder builder = Poam.Proof.newBuilder();
            try {
                JsonFormat.parser().merge(previous_json, builder);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            Poam.Proof parsed = builder.build();
            //Poam.Proof previousProof = parsed.getProofResponse();
            request = Poam.ProveRequest.newBuilder()
                    .addAllImageId(imageId)
                    .setMethodPayload(methodPayload)
                    .setPreviousProof(parsed)
                    .build();
        } else {
            System.out.println("Variable composition not set or false.");
             request = Poam.ProveRequest.newBuilder()
                    .addAllImageId(imageId)
                    .setMethodPayload(methodPayload)
                    .build();
        }

        // Make the call and receive the response
        Poam.ProveResponse response = stub.prove(request);

        // Output the result
        System.out.println("Proving result: " + response.getPublicOutput());

        // Shut down the channel
        channel.shutdown();
        return response;
    }
    public static Poam.ProveResponse single_step(ActivatedJob activatedJob){
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()  // Only for testing; don't use in production
                .build();
        VerifiableProcessingServiceGrpc.VerifiableProcessingServiceBlockingStub stub =
                VerifiableProcessingServiceGrpc.newBlockingStub(channel);
        System.out.println("Working on SingleStep Request");
        Map<String, Object> variables = activatedJob.getVariablesAsMap();
        Poam.CombinedRequest request;
        if (variables.containsKey("method_payload")) {
            List<String> methods = (List<String>) variables.get("method_payload");
            request = Poam.CombinedRequest.newBuilder()
                    .addAllMethodPayload(methods)
                    .build();
        } else {
            request = Poam.CombinedRequest.newBuilder().build();
        }

        // Make the call and receive the response
        Poam.ProveResponse response = stub.combined(request);

        // Output the result
        System.out.println("Proving result: " + response.getPublicOutput());

        // Shut down the channel
        channel.shutdown();
        return response;
    }
    public static Poam.CompositionResponse compose(ActivatedJob activatedJob){
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()  // Only for testing; don't use in production
                .build();
        VerifiableProcessingServiceGrpc.VerifiableProcessingServiceBlockingStub stub =
                VerifiableProcessingServiceGrpc.newBlockingStub(channel);
        System.out.println("Working on SingleStep Request");
        Map<String, Object> variables = activatedJob.getVariablesAsMap();
        Poam.CompositionRequest request;
        if (variables.containsKey("proof_chain")) {
            // Step 1: read the strings
            List<String> stringProofChain = (List<String>) variables.get("proof_chain");

            // Step 2: parse them back into Poam.Proof objects
            List<Poam.Proof> proofChain = new ArrayList<>();
            for (String proofJson : stringProofChain) {
                try {
                    Poam.Proof.Builder builder = Poam.Proof.newBuilder();
                    JsonFormat.parser().merge(proofJson, builder);
                    proofChain.add(builder.build());
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException("Failed to parse proof from JSON: " + proofJson, e);
                }
            }

            // Step 3: build the request
            request = Poam.CompositionRequest.newBuilder()
                    .addAllProofChain(proofChain)
                    .build();
        } else {
            request = Poam.CompositionRequest.newBuilder().build();
        }

        // Make the call and receive the response
        Poam.CompositionResponse response = stub.compose(request);

        // Output the result
        System.out.println("Proving result: " + response.getProofResponse());

        // Shut down the channel
        channel.shutdown();
        return response;
    }
}


