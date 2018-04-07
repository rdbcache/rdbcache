package doitincloud.security.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import doitincloud.commons.exceptions.ServerErrorException;
import doitincloud.commons.helpers.Utils;
import org.apache.commons.codec.digest.DigestUtils;
import org.hibernate.annotations.GenericGenerator;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import javax.persistence.*;
import java.util.*;

@Entity
@Table(name="rdbcache_user_details")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class User implements UserDetails {

    @Id
    private String username;

    private String password;

    @JsonProperty("first_name")
    @Column(name="first_name")
    private String firstName;

    @JsonProperty("last_name")
    @Column(name="last_name")
    private String lastName;

    @JsonProperty("phone_number")
    @Column(name="phone_number")
    private String phoneNumber;

    private Boolean enabled = true;

    @JsonProperty("account_non_locked")
    @Column(name="account_non_locked")
    private Boolean accountNonLocked = true;

    @JsonProperty("credentials_non_expired")
    @Column(name="credentials_non_expired")
    private Boolean credentialsNonExpired = true;

    @JsonProperty("expires_at")
    @Column(name="expires_at")
    private Long expiresAt;

    @JsonProperty("created_at")
    @Column(name="created_at", insertable=false, updatable=false)
    private Date createdAt;

    @JsonProperty("updated_at")
    @Column(name="updated_at", insertable=false, updatable=false)
    private Date updatedAt;

    @JsonIgnore
    @Transient
    private String userId;

    @JsonProperty("user_id")
    @Column(name="user_id")
    public String getUserId() {
        if (userId == null) {
            userId = Utils.generateId();
        }
        return userId;
    }

    public void setUserId(String id) {
        this.userId = id;
    }

    public void generateUserId() {
        if (userId != null) {
            throw new ServerErrorException("failed to generate user id, an id already exists");
        }
        userId = DigestUtils.md5Hex(username + System.currentTimeMillis());
    }

    @Override
    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String name) {
        this.firstName = name;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String name) {
        this.lastName = name;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @JsonIgnore
    @Transient
    @Override
    public boolean isAccountNonExpired() {
        if (expiresAt == null) {
            return true;
        }
        return System.currentTimeMillis() < expiresAt;
    }

    public void setAccountNonExpired(Boolean accountNonExpired) {
        this.expiresAt = System.currentTimeMillis();
    }

    @Override
    public boolean isAccountNonLocked() {
        return accountNonLocked;
    }

    public void setAccountNonLocked(Boolean accountNonLocked) {
        this.accountNonLocked = accountNonLocked;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return credentialsNonExpired;
    }

    public void setCredentialsNonExpired(Boolean credentialsNonExpired) {
        this.credentialsNonExpired = credentialsNonExpired;
    }

    @JsonIgnore
    @Transient
    private List<Object> authoritiesList;

    @JsonProperty("authorities")
    @Column(name="authorities")
    public String getAuthoritiesValue() {
        if (authoritiesList == null) {
            return null;
        }
        return Utils.toJson(authoritiesList);
    }

    public void setAuthoritiesValue(String value) {
        if (value == null) {
            this.authoritiesList = null;
            return;
        }
        this.authoritiesList = Utils.toList(value);
    }

    @JsonIgnore
    @Transient
    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        if (authoritiesList == null) {
            return null;
        }
        Set<GrantedAuthority> grantedAuthorities = new HashSet<>();
        for (Object role: authoritiesList) {
            grantedAuthorities.add(new SimpleGrantedAuthority((String) role));
        }
        return grantedAuthorities;
    }

    public void addRole(String role) {
        if (authoritiesList == null) {
            authoritiesList = new ArrayList<>();
        }
        authoritiesList.add(role);
    }

    @JsonIgnore
    @Transient
    private Map<String, Object> additionalInformationMap;

    @JsonProperty("additional_information")
    @Column(name="additional_information")
    public String getAdditionalInformationValue() {
        if (additionalInformationMap == null) {
            return null;
        }
        return Utils.toJson(additionalInformationMap);
    }

    public void setAdditionalInformationValue(String value) {
        if (value == null) {
            additionalInformationMap = null;
            return;
        }
        additionalInformationMap = Utils.toMap(value);
    }

    @JsonIgnore
    @Transient
    public Map<String, Object> getAdditionalInformation() {
        return additionalInformationMap;
    }

    @JsonIgnore
    @Transient
    public void setAdditionalInformation(Map<String, Object> map) {
        this.additionalInformationMap = map;
    }

    public void putAdditionalInformation(String key, Object value) {
        if (additionalInformationMap == null) {
            additionalInformationMap = new LinkedHashMap<>();
        }
        additionalInformationMap.put(key, value);
    }

    public Long getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Long expiresAt) {
        this.expiresAt = expiresAt;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }
}
