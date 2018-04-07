package doitincloud.oauth2.services;

import doitincloud.oauth2.models.Client;
import doitincloud.oauth2.forms.SignupInfo;

public interface ClientService {

    Client getByClientId(String clientId);

    Client createNewClient(SignupInfo signupInfo);

    void update(Client client);

    String findCurrentClientId();

}