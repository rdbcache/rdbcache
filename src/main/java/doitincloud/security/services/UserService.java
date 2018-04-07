package doitincloud.security.services;

import doitincloud.security.forms.SignupInfo;
import doitincloud.security.models.User;

import java.util.Map;

public interface UserService {

    Map<String, String> getAvailableRoles();

    User getUserByEmail(String email);

    User createNewUser(SignupInfo signupInfo);

    void update(User user);

    String findCurrentUsername();

    void autologin(String username, String password);

}
