package doitincloud.security.repositories;

import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.web.authentication.rememberme.PersistentRememberMeToken;
import org.springframework.security.web.authentication.rememberme.PersistentTokenRepository;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Repository
@Transactional
public class RememberMeTokenRepo implements PersistentTokenRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(RememberMeTokenRepo.class);

    private static String typeToken = "RememberMeToken";

    private static String typeUsername = "RememberMeUsername";

    private final Map<String, PersistentRememberMeToken> seriesTokens = new HashMap<>();

    @Override
    public void createNewToken(PersistentRememberMeToken token) {

        LOGGER.trace("call createNewToken with " + token.getUsername());

        String seriesId = token.getSeries();
        PersistentRememberMeToken tokenOld = getTokenForSeries(seriesId);
        if (tokenOld != null) {
            throw new DataIntegrityViolationException("Series Id '" + seriesId
                    + "' already exists!");
        }
        KvPair pair = new KvPair(seriesId, typeToken, Utils.toMap(token));
        AppCtx.getKvPairRepo().save(pair);

        KvIdType idType = new KvIdType(token.getUsername(), typeUsername);
        Optional<KvPair> pairOpt = AppCtx.getKvPairRepo().findById(idType);
        pair = null;
        if (pairOpt.isPresent()) {
            pair = pairOpt.get();
        }
        Map<String, Object> map = null;
        if (pair == null) {
            pair = new KvPair(idType);
            map = new LinkedHashMap<>();
            pair.setData(map);
        } else {
            map = pair.getData();
        }
        map.put(seriesId, System.currentTimeMillis());
        AppCtx.getKvPairRepo().save(pair);
    }

    @Override
    public void updateToken(String seriesId, String tokenValue, Date lastUsed) {

        LOGGER.trace("call updateToken with " + seriesId);

        PersistentRememberMeToken token = getTokenForSeries(seriesId);
        if (token == null) {
            throw new DataIntegrityViolationException("Series Id '" + seriesId
                    + "' not exists!");
        }
        PersistentRememberMeToken newToken = new PersistentRememberMeToken(
                token.getUsername(), seriesId, tokenValue, new Date());
        KvPair pair = new KvPair(seriesId, typeToken, Utils.toMap(newToken));
        AppCtx.getKvPairRepo().save(pair);
    }

    @Override
    public PersistentRememberMeToken getTokenForSeries(String seriesId) {

        LOGGER.trace("call getTokenForSeries with " + seriesId);

        KvIdType idType = new KvIdType(seriesId, typeToken);
        Optional<KvPair> pairOpt = AppCtx.getKvPairRepo().findById(idType);
        if (!pairOpt.isPresent()) {
            return null;
        }
        KvPair pair = pairOpt.get();
        PersistentRememberMeToken token = Utils.toPojo(pair.getData(), PersistentRememberMeToken.class);
        return token;
    }

    @Override
    public void removeUserTokens(String username) {

        LOGGER.trace("call removeUserTokens with " + username);

        KvIdType idType = new KvIdType(username, typeUsername);
        Optional<KvPair> pairOpt = AppCtx.getKvPairRepo().findById(idType);
        if (!pairOpt.isPresent()) {
            return;
        }
        KvPair pair = pairOpt.get();
        Map<String, Object> map = pair.getData();
        Set<String> seriesIdSet = map.keySet();

        for (String seriesId: seriesIdSet) {
            KvIdType kvIdType = new KvIdType(seriesId, typeToken);
            AppCtx.getKvPairRepo().deleteById(kvIdType);
        }
        AppCtx.getKvPairRepo().deleteById(idType);
    }
}
