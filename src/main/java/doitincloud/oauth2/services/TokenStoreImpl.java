package doitincloud.oauth2.services;

import doitincloud.oauth2.repositories.TokenRepo;
import doitincloud.oauth2.repositories.impls.TokenRepoImpl;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.OAuth2RefreshToken;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.AuthenticationKeyGenerator;
import org.springframework.security.oauth2.provider.token.DefaultAuthenticationKeyGenerator;
import org.springframework.security.oauth2.provider.token.TokenStore;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TokenStoreImpl implements TokenStore {

    private static final int DEFAULT_FLUSH_INTERVAL = 1000;

    private final TokenRepo<OAuth2AccessToken> accessTokenRepo =
            new TokenRepoImpl<OAuth2AccessToken>("accessToken", OAuth2AccessToken.class);

    private final TokenRepo<OAuth2AccessToken> authenticationToAccessTokenRepo =
            new TokenRepoImpl<OAuth2AccessToken>("authenticationToAccessToken", OAuth2AccessToken.class);

    private final TokenRepo<OAuth2AccessToken> userNameToAccessTokenRepo =
            new TokenRepoImpl<OAuth2AccessToken>("userNameToAccessToken", OAuth2AccessToken.class);

    private final TokenRepo<OAuth2AccessToken> clientIdToAccessTokenRepo =
            new TokenRepoImpl<OAuth2AccessToken>("clientIdToAccessToken", OAuth2AccessToken.class);

    private final TokenRepo<OAuth2RefreshToken> refreshTokenRepo =
            new TokenRepoImpl<OAuth2RefreshToken>("refreshToken", OAuth2RefreshToken.class);

    private final TokenRepo<String> accessTokenToRefreshTokenRepo =
            new TokenRepoImpl<String>("accessTokenToRefreshToken", String.class);

    private final TokenRepo<OAuth2Authentication> authenticationStore =
            new TokenRepoImpl<OAuth2Authentication>("authentication", OAuth2Authentication.class);

    private final TokenRepo<OAuth2Authentication> refreshTokenAuthenticationStore =
            new TokenRepoImpl<OAuth2Authentication>("refreshTokenAuthentication", OAuth2Authentication.class);

    private final TokenRepo<String> refreshTokenToAccessTokenRepo =
            new TokenRepoImpl<String>("refreshTokenToAccessToken", String.class);

    private final TokenRepo<TokenExpiry> expiryMap =
            new TokenRepoImpl<TokenExpiry>("expiryMap", TokenExpiry.class);

    private final DelayQueue<TokenExpiry> expiryQueue = new DelayQueue<TokenExpiry>();

    private int flushInterval = DEFAULT_FLUSH_INTERVAL;

    private AuthenticationKeyGenerator authenticationKeyGenerator = new DefaultAuthenticationKeyGenerator();

    private AtomicInteger flushCounter = new AtomicInteger(0);

    /**
     * The number of tokens to store before flushing expired tokens. Defaults to 1000.
     *
     * @param flushInterval the interval to set
     */
    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    /**
     * The interval (count of token inserts) between flushing expired tokens.
     *
     * @return the flushInterval the flush interval
     */
    public int getFlushInterval() {
        return flushInterval;
    }

    public void setAuthenticationKeyGenerator(AuthenticationKeyGenerator authenticationKeyGenerator) {
        this.authenticationKeyGenerator = authenticationKeyGenerator;
    }

    public int getExpiryTokenCount() {
        return expiryQueue.size();
    }

    @Override
    public OAuth2AccessToken getAccessToken(OAuth2Authentication authentication) {
        String key = authenticationKeyGenerator.extractKey(authentication);
        OAuth2AccessToken accessToken = authenticationToAccessTokenRepo.get(key);
        if (accessToken != null
                && !key.equals(authenticationKeyGenerator.extractKey(readAuthentication(accessToken.getValue())))) {
            // Keep the stores consistent (maybe the same user is represented by this authentication but the details
            // have changed)
            storeAccessToken(accessToken, authentication);
        }
        return accessToken;
    }

    @Override
    public OAuth2Authentication readAuthentication(OAuth2AccessToken token) {
        return readAuthentication(token.getValue());
    }

    @Override
    public OAuth2Authentication readAuthentication(String token) {
        return this.authenticationStore.get(token);
    }

    @Override
    public OAuth2Authentication readAuthenticationForRefreshToken(OAuth2RefreshToken token) {
        return readAuthenticationForRefreshToken(token.getValue());
    }

    public OAuth2Authentication readAuthenticationForRefreshToken(String token) {
        return this.refreshTokenAuthenticationStore.get(token);
    }

    @Override
    public void storeAccessToken(OAuth2AccessToken token, OAuth2Authentication authentication) {
        if (this.flushCounter.incrementAndGet() >= this.flushInterval) {
            flush();
            this.flushCounter.set(0);
        }
        this.accessTokenRepo.put(token.getValue(), token);
        this.authenticationStore.put(token.getValue(), authentication);
        this.authenticationToAccessTokenRepo.put(authenticationKeyGenerator.extractKey(authentication), token);
        if (!authentication.isClientOnly()) {
            addToCollection(this.userNameToAccessTokenRepo, getApprovalKey(authentication), token);
        }
        addToCollection(this.clientIdToAccessTokenRepo, authentication.getOAuth2Request().getClientId(), token);
        if (token.getExpiration() != null) {
            TokenExpiry expiry = new TokenExpiry(token.getValue(), token.getExpiration());
            // Remove existing expiry for this token if present
            expiryQueue.remove(expiryMap.put(token.getValue(), expiry));
            this.expiryQueue.put(expiry);
        }
        if (token.getRefreshToken() != null && token.getRefreshToken().getValue() != null) {
            this.refreshTokenToAccessTokenRepo.put(token.getRefreshToken().getValue(), token.getValue());
            this.accessTokenToRefreshTokenRepo.put(token.getValue(), token.getRefreshToken().getValue());
        }
    }

    private String getApprovalKey(OAuth2Authentication authentication) {
        String userName = authentication.getUserAuthentication() == null ? "" : authentication.getUserAuthentication()
                .getName();
        return getApprovalKey(authentication.getOAuth2Request().getClientId(), userName);
    }

    private String getApprovalKey(String clientId, String userName) {
        return clientId + (userName==null ? "" : ":" + userName);
    }

    private void addToCollection(TokenRepo<OAuth2AccessToken> store, String key,
                                 OAuth2AccessToken token) {
        Collection<OAuth2AccessToken> tokens = store.getCollection(key);
        if (tokens == null || tokens.size() == 0) {
            store.put(key, token);
        } else {
            tokens.add(token);
            store.put(key, tokens);
        }
    }

    @Override
    public void removeAccessToken(OAuth2AccessToken accessToken) {
        removeAccessToken(accessToken.getValue());
    }

    @Override
    public OAuth2AccessToken readAccessToken(String tokenValue) {
        return this.accessTokenRepo.get(tokenValue);
    }

    public void removeAccessToken(String tokenValue) {
        OAuth2AccessToken removed = this.accessTokenRepo.remove(tokenValue);
        this.accessTokenToRefreshTokenRepo.remove(tokenValue);
        // Don't remove the refresh token - it's up to the caller to do that
        OAuth2Authentication authentication = this.authenticationStore.remove(tokenValue);
        if (authentication != null) {
            this.authenticationToAccessTokenRepo.remove(authenticationKeyGenerator.extractKey(authentication));
            Collection<OAuth2AccessToken> tokens;
            String clientId = authentication.getOAuth2Request().getClientId();
            tokens = this.userNameToAccessTokenRepo.getCollection(getApprovalKey(clientId, authentication.getName()));
            if (tokens != null) {
                tokens.remove(removed);
            }
            tokens = this.clientIdToAccessTokenRepo.getCollection(clientId);
            if (tokens != null) {
                tokens.remove(removed);
            }
            this.authenticationToAccessTokenRepo.remove(authenticationKeyGenerator.extractKey(authentication));
        }
    }

    @Override
    public void storeRefreshToken(OAuth2RefreshToken refreshToken, OAuth2Authentication authentication) {
        this.refreshTokenRepo.put(refreshToken.getValue(), refreshToken);
        this.refreshTokenAuthenticationStore.put(refreshToken.getValue(), authentication);
    }

    @Override
    public OAuth2RefreshToken readRefreshToken(String tokenValue) {
        return this.refreshTokenRepo.get(tokenValue);
    }

    @Override
    public void removeRefreshToken(OAuth2RefreshToken refreshToken) {
        removeRefreshToken(refreshToken.getValue());
    }

    public void removeRefreshToken(String tokenValue) {
        this.refreshTokenRepo.remove(tokenValue);
        this.refreshTokenAuthenticationStore.remove(tokenValue);
        this.refreshTokenToAccessTokenRepo.remove(tokenValue);
    }

    @Override
    public void removeAccessTokenUsingRefreshToken(OAuth2RefreshToken refreshToken) {
        removeAccessTokenUsingRefreshToken(refreshToken.getValue());
    }

    private void removeAccessTokenUsingRefreshToken(String refreshToken) {
        String accessToken = this.refreshTokenToAccessTokenRepo.remove(refreshToken);
        if (accessToken != null) {
            removeAccessToken(accessToken);
        }
    }

    @Override
    public Collection<OAuth2AccessToken> findTokensByClientIdAndUserName(String clientId, String userName) {
        Collection<OAuth2AccessToken> result = userNameToAccessTokenRepo.getCollection(getApprovalKey(clientId, userName));
        return result != null ? Collections.<OAuth2AccessToken> unmodifiableCollection(result) : Collections
                .<OAuth2AccessToken> emptySet();
    }

    @Override
    public Collection<OAuth2AccessToken> findTokensByClientId(String clientId) {
        Collection<OAuth2AccessToken> result = clientIdToAccessTokenRepo.getCollection(clientId);
        return result != null ? Collections.<OAuth2AccessToken> unmodifiableCollection(result) : Collections
                .<OAuth2AccessToken> emptySet();
    }

    private void flush() {
        TokenExpiry expiry = expiryQueue.poll();
        while (expiry != null) {
            removeAccessToken(expiry.getValue());
            expiry = expiryQueue.poll();
        }
    }

    private static class TokenExpiry implements Delayed {

        private final long expiry;

        private final String value;

        public TokenExpiry(String value, Date date) {
            this.value = value;
            this.expiry = date.getTime();
        }

        public int compareTo(Delayed other) {
            if (this == other) {
                return 0;
            }
            long diff = getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS);
            return (diff == 0 ? 0 : ((diff < 0) ? -1 : 1));
        }

        public long getDelay(TimeUnit unit) {
            return expiry - System.currentTimeMillis();
        }

        public String getValue() {
            return value;
        }

    }
}
