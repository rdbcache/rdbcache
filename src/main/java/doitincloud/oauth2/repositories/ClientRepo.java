package doitincloud.oauth2.repositories;

import doitincloud.oauth2.models.Client;

public interface ClientRepo {

    Client findByClientId(String clientId);

    void save(Client client);

    void update(Client client);

    void delete(String clientId);

}
