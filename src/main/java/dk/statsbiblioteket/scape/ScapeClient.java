package dk.statsbiblioteket.scape;

import co.paralleluniverse.fibers.ws.rs.client.AsyncClientBuilder;

import javax.ws.rs.client.Client;

public interface ScapeClient {


    public static final Client httpClient = AsyncClientBuilder.newClient();
}
