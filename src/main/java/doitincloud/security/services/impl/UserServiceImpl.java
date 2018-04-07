package doitincloud.security.services.impl;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.security.forms.SignupInfo;
import doitincloud.security.models.User;
import doitincloud.security.repositories.UserRepo;
import doitincloud.security.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.crypto.password.PasswordEncoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class UserServiceImpl implements UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserServiceImpl.class);

    @Autowired
    private UserRepo userRepo;

    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    public PasswordEncoder passwordEncoder;

    @Override
    public Map<String, String> getAvailableRoles() {
        Map<String, String> mapRoles = new LinkedHashMap<>();
        mapRoles.put("ROLE_USER", "normal user - allow web access");
        mapRoles.put("ROLE_POWER", "power user - allow web + api access");
        mapRoles.put("ROLE_ADMIN", "normal admin - manage users");
        mapRoles.put("ROLE_SUPER", "super admin - manage admins");
        return mapRoles;
    }

    @Override
    public User getUserByEmail(String email) {
        return userRepo.findByUsername(email);
    }

    @Override
    public User createNewUser(SignupInfo signupInfo) {

        String username = signupInfo.getEmail();
        if (username == null) {
            throw new ServerErrorException("username can not be null");
        }
        String password = signupInfo.getPassword();
        if (password == null) {
            throw new ServerErrorException("password can not be null");
        }

        User user = userRepo.findByUsername(username);
        if (user != null) {
            return null;
        }

        user = new User();

        user.setUsername(username);
        user.setPassword(passwordEncoder.encode(password));

        String firstName = signupInfo.getFirstName();
        if (firstName != null) {
            user.setFirstName(firstName);
        }
        String lastName = signupInfo.getLastName();
        if (lastName != null) {
            user.setLastName(lastName);
        }
        String phoneNumber = signupInfo.getPhoneNumber();
        if (phoneNumber != null) {
            user.setPhoneNumber(phoneNumber);
        }
        List<String> roles = signupInfo.getRoles();
        if (roles != null && roles.size() > 0) {
            for (String role: roles) {
                if (role.length() == 0) {
                    continue;
                }
                user.addRole(role);
            }
        }

        userRepo.save(user);

        return user;
    }

    @Override
    public void update(User user) {
        userRepo.update(user);
    }

    @Override
    public String findCurrentUsername() {

        Object userDetails = SecurityContextHolder.getContext().getAuthentication().getDetails();
        if (userDetails instanceof UserDetails) {
            return ((UserDetails)userDetails).getUsername();
        }
        return null;
    }

    @Override
    public void autologin(String username, String password) {

        UserDetails userDetails = userRepo.findByUsername(username);
        UsernamePasswordAuthenticationToken token =
                new UsernamePasswordAuthenticationToken(userDetails, password, userDetails.getAuthorities());

        authenticationManager.authenticate(token);

        if (token.isAuthenticated()) {
            SecurityContextHolder.getContext().setAuthentication(token);
            LOGGER.debug(String.format("Auto login %s successfully!", username));
        }
    }
}
