package dk.statsbiblioteket.scape;


import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;

public abstract class ScapeClient {


    public static final Client httpClient = JerseyClientBuilder.newClient();
    private String service;
    private String username;
    private String password;

    protected ScapeClient(String service, String username, String password) {
        this.service = service;
        this.username = username;
        this.password = password;
    }

    protected WebTarget request() {
        HttpAuthenticationFeature feature = HttpAuthenticationFeature.basicBuilder().credentials(username,password).build();
        return httpClient.target(service).register(feature);
    }

}
