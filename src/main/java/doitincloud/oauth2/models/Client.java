package doitincloud.oauth2.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import doitincloud.commons.exceptions.ServerErrorException;
import org.hibernate.annotations.GenericGenerator;


import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.provider.ClientDetails;

import doitincloud.commons.helpers.Utils;

import javax.persistence.*;
import java.util.*;

@Entity
@Table(name="rdbcache_client_details")
public class Client implements ClientDetails {

    @Id
    @JsonProperty("client_id")
    @Column(name="client_id")
    private String clientId;

    @JsonProperty("secret_required")
    @Column(name="secret_required")
    private Boolean secretRequired;

    @JsonProperty("client_secret")
    @Column(name="client_secret")
    private String clientSecret;

    private Boolean scoped;

    @JsonProperty("access_token_validity_seconds")
    @Column(name="access_token_validity_seconds")
    private Integer accessTokenValiditySeconds;

    @JsonProperty("refresh_token_validity_seconds")
    @Column(name="refresh_token_validity_seconds")
    private Integer refreshTokenValiditySeconds;

    @JsonProperty("expires_at")
    @Column(name="expires_at")
    private Long expiresAt;

    @JsonProperty("created_at")
    @Column(name="created_at", insertable=false, updatable=false)
    private Date createdAt;

    @JsonProperty("updated_at")
    @Column(name="updated_at", insertable=false, updatable=false)
    private Date updatedAt;

    @Override
    public String getClientId() {
        if (clientId == null) {
            clientId = Utils.generateId();
        }
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void generateClientId() {
        if (clientId != null) {
            throw new ServerErrorException("failed to generate client id, an id already exists");
        }
        clientId = Utils.generateId();
    }

    @JsonIgnore
    @Transient
    private Set<String> resourceIdsSet;

    @JsonProperty("resource_ids")
    @Column(name="resource_ids")
    public String getResourceIdsValue() {
        if (resourceIdsSet == null) {
            return null;
        }
        return Utils.toJson(resourceIdsSet);
    }

    public void setResourceIdsValue(String value) {
        if (value == null) {
            resourceIdsSet = null;
            return;
        }
        if (resourceIdsSet == null) {
            resourceIdsSet = new HashSet<>();
        }
        List<Object> list = Utils.toList(value);
        for (Object resourceId : list) {
            resourceIdsSet.add((String) resourceId);
        }
    }

    @JsonIgnore
    @Override
    public Set<String> getResourceIds() {
        return resourceIdsSet;
    }

    public void setResourceIds(Set<String> resourceIds) {
        this.resourceIdsSet = resourceIds;
    }

    @Override
    public boolean isSecretRequired() {
        return secretRequired;
    }

    public void setSecretRequired(Boolean secretRequired) {
        this.secretRequired = secretRequired;
    }

    @Override
    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    @Override
    public boolean isScoped() {
        return scoped;
    }

    public void setScoped(Boolean scoped) {
        this.scoped = scoped;
    }

    @JsonIgnore
    @Transient
    private Set<String> scopeSet;

    @JsonProperty("scope")
    @Column(name="scope")
    public String getScopeValue() {
        if (scopeSet == null) {
            return null;
        }
        return Utils.toJson(scopeSet);
    }

    public void setScopeValue(String value) {
        if (value == null) {
            scopeSet = null;
            return;
        }
        if (scopeSet == null) {
            scopeSet = new HashSet<>();
        }
        List<Object> list = Utils.toList(value);
        for (Object scope: list) {
            scopeSet.add((String) scope);
        }
    }

    @Override
    public Set<String> getScope() {
        return scopeSet;
    }

    public void setScope(Set<String> scope) {
        this.scopeSet = scope;
    }

    @JsonIgnore
    @Transient
    private Set<String> authorizedGrantTypesSet;

    @JsonProperty("authorized_grant_types")
    @Column(name="authorized_grant_types")
    public String getAuthorizedGrantTypesValue() {
        if (authorizedGrantTypesSet == null) {
            return null;
        }
        return Utils.toJson(authorizedGrantTypesSet);
    }

    public void setAuthorizedGrantTypesValue(String value) {
        if (value == null) {
            authorizedGrantTypesSet = null;
            return;
        }
        if (authorizedGrantTypesSet == null) {
            authorizedGrantTypesSet = new HashSet<>();
        }
        List<Object> list = Utils.toList(value);
        for (Object scope: list) {
            authorizedGrantTypesSet.add((String) scope);
        }
    }

    @Override
    public Set<String> getAuthorizedGrantTypes() {
        return authorizedGrantTypesSet;
    }

    public void setAuthorizedGrantTypes(Set<String> grantTypes) {
        authorizedGrantTypesSet = grantTypes;
    }

    @Override
    public Integer getAccessTokenValiditySeconds() {
        return accessTokenValiditySeconds;
    }

    public void setAccessTokenValiditySeconds(Integer accessTokenValiditySeconds) {
        this.accessTokenValiditySeconds = accessTokenValiditySeconds;
    }

    @Override
    public Integer getRefreshTokenValiditySeconds() {
        return refreshTokenValiditySeconds;
    }

    public void setRefreshTokenValiditySeconds(Integer refreshTokenValiditySeconds) {
        this.refreshTokenValiditySeconds = refreshTokenValiditySeconds;
    }

    @JsonIgnore
    @Transient
    private Set<String> autoApproveScopesSet;

    @JsonProperty("auto_approve_scopes")
    @Column(name="auto_approve_scopes")
    public String getAutoApproveScopesValue() {
        if (autoApproveScopesSet == null) {
            return null;
        }
        return Utils.toJson(autoApproveScopesSet);
    }

    public void setAutoApproveScopesValue(String value) {
        if (value == null) {
            autoApproveScopesSet = null;
            return;
        }
        if (autoApproveScopesSet == null) {
            autoApproveScopesSet = new HashSet<>();
        }
        List<Object> list = Utils.toList(value);
        for (Object scope: list) {
            autoApproveScopesSet.add((String) scope);
        }
    }

    public Set<String> getAutoApproveScopes() {
        return autoApproveScopesSet;
    }

    public void setAutoApproveScopes(Set<String> autoApproveScopes) {
        this.autoApproveScopesSet = autoApproveScopes;
    }

    @Override
    public boolean isAutoApprove(String scope) {
        if (autoApproveScopesSet == null) {
            return false;
        }
        return autoApproveScopesSet.contains(scope);
    }

    @JsonIgnore
    @Transient
    private Set<String> registeredRedirectUriSet;

    @JsonProperty("registered_redirect_uri")
    @Column(name="registered_redirect_uri")
    public String getRegisteredRedirectUriValue() {
        if (registeredRedirectUriSet == null) {
            return null;
        }
        return Utils.toJson(registeredRedirectUriSet);
    }

    public void setRegisteredRedirectUriValue(String value) {
        if (value == null) {
            registeredRedirectUriSet = null;
            return;
        }
        if (registeredRedirectUriSet == null) {
            registeredRedirectUriSet = new HashSet<>();
        }
        List<Object> list = Utils.toList(value);
        for (Object scope: list) {
            registeredRedirectUriSet.add((String) scope);
        }
    }

    @Override
    public Set<String> getRegisteredRedirectUri() {
        return registeredRedirectUriSet;
    }

    public void setRegisteredRedirectUri(Set<String> registeredRedirectUri) {
        this.registeredRedirectUriSet = registeredRedirectUri;
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
    public Collection<GrantedAuthority> getAuthorities() {
        if (authoritiesList == null) {
            return null;
        }
        Set<GrantedAuthority> grantedAuthorities = new HashSet<>();
        for (java.lang.Object role: authoritiesList) {
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

