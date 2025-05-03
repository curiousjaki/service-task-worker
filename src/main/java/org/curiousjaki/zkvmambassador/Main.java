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
            Map<String, Object> variables = new HashMap<>();
            variables.put("public_output", public_outout);
            try {
                variables.put("previous_proof", JsonFormat.printer().print(response));
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
            Poam.ProveResponse response = combine(job);
            String public_outout = response.getPublicOutput();
            Poam.Proof proofResult = response.getProofResponse();
            // Create output variables
            Map<String, Object> variables = new HashMap<>();
            variables.put("public_output", public_outout);
            try {
                variables.put("previous_proof", JsonFormat.printer().print(response));
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
            Poam.ProveResponse.Builder builder = Poam.ProveResponse.newBuilder();
            try {
                JsonFormat.parser().merge(previous_json, builder);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            Poam.ProveResponse parsed = builder.build();
            Poam.Proof previousProof = parsed.getProofResponse();
            request = Poam.VerifyRequest.newBuilder()
                    .setProof(previousProof)
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
        if (variables.containsKey("previous_proof")) {
            String previous_json = variables.get("previous_proof").toString();
            Poam.ProveResponse.Builder builder = Poam.ProveResponse.newBuilder();
            try {
                JsonFormat.parser().merge(previous_json, builder);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
            Poam.ProveResponse parsed = builder.build();
            Poam.Proof previousProof = parsed.getProofResponse();
            request = Poam.ProveRequest.newBuilder()
                    .addAllImageId(imageId)
                    .setMethodPayload(methodPayload)
                    .setPreviousProof(previousProof)
                    .build();
        } else {
            System.out.println("Variable 'previous_public_output' is not available.");
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
    public static Poam.ProveResponse combine(ActivatedJob activatedJob){
        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50051)
                .usePlaintext()  // Only for testing; don't use in production
                .build();
        VerifiableProcessingServiceGrpc.VerifiableProcessingServiceBlockingStub stub =
                VerifiableProcessingServiceGrpc.newBlockingStub(channel);
        System.out.println("Working on Combined Request");
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
}


