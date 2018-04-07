package doitincloud.oauth2.services.impls;

import doitincloud.oauth2.forms.SignupInfo;
import doitincloud.oauth2.models.Client;
import doitincloud.oauth2.repositories.ClientRepo;
import doitincloud.oauth2.services.ClientService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.crypto.password.PasswordEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientServiceImpl implements ClientService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientServiceImpl.class);

    @Autowired
    private ClientRepo clientRepo;

    //@Autowired
    //private AuthenticationManager authenticationManager;

    @Autowired
    public PasswordEncoder passwordEncoder;

    @Override
    public Client getByClientId(String clientId) {
        return clientRepo.findByClientId(clientId);
    }

    @Override
    public Client createNewClient(SignupInfo signupInfo) {
        return null;
    }

    @Override
    public void update(Client client) {
        clientRepo.update(client);
    }

    @Override
    public String findCurrentClientId() {
        return null;
    }
}
