package com.rs;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AppTest {

    private final static String ENDPOINT = "";
    private final static String READ_WRITE_KEY = "";
    private final static String READONLY_KEY = "";
    private final static String CONTAINER = "addresses_1";
    private final static String DB = "demo";

    private static CosmosClient jpeWriteClient, jpeReadClient;
    private static CosmosClient jpwWriteClient, jpwReadClient;

    private static CosmosContainer jpeWriteContainer, jpeReadContainer;
    private static CosmosContainer jpwWriteContainer, jpwReadContainer;

    private static final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private static final Timer writeTimer = registry.timer("writes");
    private static final Timer readTimer = registry.timer("reads");
    private static final DistributionSummary distributionSummary = DistributionSummary.builder("summary").register(registry);

    @BeforeAll
    public static void init() throws IOException {

        jpeWriteClient = new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .gatewayMode()
                .key(READ_WRITE_KEY)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .multipleWriteRegionsEnabled(true)
                .endpointDiscoveryEnabled(true)
                .preferredRegions(Collections.singletonList("Japan East"))
                .buildClient();

        jpeReadClient = new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .gatewayMode()
                .key(READONLY_KEY)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .endpointDiscoveryEnabled(true)
                .preferredRegions(Collections.singletonList("Japan East"))
                .buildClient();

        jpwWriteClient = new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .gatewayMode()
                .key(READ_WRITE_KEY)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .multipleWriteRegionsEnabled(true)
                .endpointDiscoveryEnabled(true)
                .preferredRegions(Collections.singletonList("Japan West"))
                .buildClient();

        jpwReadClient = new CosmosClientBuilder()
                .endpoint(ENDPOINT)
                .gatewayMode()
                .key(READONLY_KEY)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .endpointDiscoveryEnabled(true)
                .preferredRegions(Collections.singletonList("Japan West"))
                .buildClient();

        jpeWriteContainer = jpeWriteClient.getDatabase(DB).getContainer(CONTAINER);
        jpeReadContainer = jpeReadClient.getDatabase(DB).getContainer(CONTAINER);
        jpwWriteContainer = jpwWriteClient.getDatabase(DB).getContainer(CONTAINER);
        jpwReadContainer = jpwReadClient.getDatabase(DB).getContainer(CONTAINER);
    }

    @AfterAll
    public static void summarize() {
        System.out.println("Total Write latency - " + writeTimer.mean(TimeUnit.MILLISECONDS));
        System.out.println("Total Read latency - " + readTimer.mean(TimeUnit.MILLISECONDS));
    }

    private void writeItem(CosmosContainer container, Address address) {
        writeTimer.record(() -> {
            container.createItem(address, new PartitionKey(address.getPostalCode()), new CosmosItemRequestOptions());
        });
    }

    private void readItem(CosmosContainer container, String id, String partitionKey) {
        readTimer.wrap(() -> {
           container.readItem(id, new PartitionKey(partitionKey), Address.class);
        });
    }

    @DisplayName("Ping Pong")
    @Test
    public void ping() {
        System.out.println("pong");
    }

    @DisplayName("Write in JPE & Read from JPW")
    @Test
    public void testJapanEast() {
        for (int i = 1; i <= 1000000; i++) {
            String id = "" + i;
            String postalCode = "Postcode " + i;
            String city = "City " + i;
            String prefecture = "Prefecture " + i;
            String street = "Street " + i;

            Address address = new Address(id, prefecture, city, street, postalCode);
            writeItem(jpeWriteContainer, address);
            readItem(jpwReadContainer, id, postalCode);
        }
    }
}