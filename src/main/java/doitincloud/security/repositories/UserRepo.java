package doitincloud.security.repositories;

import doitincloud.security.models.User;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepo {

    User findByUsername(String username);

    void save(User user);

    void update(User user);

    void delete(String username);
}
