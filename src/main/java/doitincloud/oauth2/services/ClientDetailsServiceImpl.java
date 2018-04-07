package doitincloud.oauth2.services;

import doitincloud.oauth2.models.Client;
import doitincloud.oauth2.repositories.ClientRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.oauth2.provider.ClientDetails;
import org.springframework.security.oauth2.provider.ClientDetailsService;
import org.springframework.security.oauth2.provider.ClientRegistrationException;

public class ClientDetailsServiceImpl implements ClientDetailsService {

    @Autowired
    ClientRepo clientRepo;

    @Override
    public ClientDetails loadClientByClientId(String clientId) throws ClientRegistrationException {

        Client client = clientRepo.findByClientId(clientId);
        if (client != null) {
            return client;
        }
        throw new ClientRegistrationException("client " + clientId + " not found");
    }

}
