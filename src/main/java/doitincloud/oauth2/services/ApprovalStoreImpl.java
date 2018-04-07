package doitincloud.oauth2.services;

import doitincloud.commons.helpers.Context;
import doitincloud.commons.helpers.Utils;
import doitincloud.rdbcache.configs.AppCtx;
import doitincloud.rdbcache.models.KeyInfo;
import doitincloud.rdbcache.models.KvIdType;
import doitincloud.rdbcache.models.KvPair;
import org.springframework.security.oauth2.provider.approval.Approval;
import org.springframework.security.oauth2.provider.approval.ApprovalStore;

import java.util.*;

public class ApprovalStoreImpl implements ApprovalStore {

    private static String type = "approval";

    private Context getContext() {
        Context context = new Context();
        return context;
    }

    @Override
    public boolean addApprovals(Collection<Approval> approvals) {

        boolean allOk = true;
        Context context = getContext();
        Map<String, List<Object>> todoMap = new LinkedHashMap<>();

        for (Approval approval: approvals) {

            String hashKey = approval.getClientId() + "/" + approval.getUserId();
            KvIdType idType = new KvIdType(hashKey, type);

            Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
            if (map == null) {
                KvPair pair = new KvPair(idType);
                KeyInfo keyInfo = new KeyInfo();
                if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
                    map = pair.getData();
                } else if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
                    map = pair.getData();
                }
            }
            List<Object> todoList = todoMap.get(hashKey);
            if (todoList == null) {
                todoList = new ArrayList<>();
                todoMap.put(hashKey, todoList);
            }
            if (map != null) {
                List<Object> list = Utils.convertMapToList(map);
                for (Object object: list) {
                    if (!todoList.contains(object)) {
                        todoList.add(object);
                    }
                }
            }
            Map<String, Object> approvalMap = Utils.toMap(approval);
            if (!todoList.contains(approvalMap)) {
                todoList.add(approvalMap);
            }
        }
        for (Map.Entry<String, List<Object>> entry: todoMap.entrySet()) {
            String hashKey = entry.getKey();
            List<Object> value = entry.getValue();
            Map<String, Object> map = Utils.convertListToMap(value);
            KvPair pair = new KvPair(hashKey, "approval", map);
            KeyInfo keyInfo = new KeyInfo();
            if (!AppCtx.getRedisRepo().save(context, pair, keyInfo)) {
                allOk = false;
            }
            Utils.getExcutorService().submit(() -> {
                AppCtx.getDbaseRepo().save(context, pair, keyInfo);
            });
        }
        return allOk;
    }

    @Override
    public boolean revokeApprovals(Collection<Approval> approvals) {

        boolean allOk = true;
        Context context = getContext();
        Map<String, List<Object>> todoMap = new LinkedHashMap<>();

        for (Approval approval: approvals) {

            String hashKey = approval.getClientId() + "/" + approval.getUserId();
            KvIdType idType = new KvIdType(hashKey, type);

            Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
            if (map == null) {
                KvPair pair = new KvPair(idType);
                KeyInfo keyInfo = new KeyInfo();
                if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
                    map = pair.getData();
                } else if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
                    map = pair.getData();
                }
            }
            List<Object> todoList = todoMap.get(hashKey);
            if (todoList == null) {
                todoList = new ArrayList<>();
                todoMap.put(hashKey, todoList);
            }
            if (map != null) {
                List<Object> list = Utils.convertMapToList(map);
                for (Object object: list) {
                    if (!todoList.contains(object)) {
                        todoList.add(object);
                    }
                }
            }
            Map<String, Object> approvalMap = Utils.toMap(approval);
            if (todoList.contains(approvalMap)) {
                todoList.remove(approvalMap);
            }
        }
        for (Map.Entry<String, List<Object>> entry: todoMap.entrySet()) {
            String hashKey = entry.getKey();
            List<Object> value = entry.getValue();
            Map<String, Object> map = Utils.convertListToMap(value);
            KvIdType idType = new KvIdType(hashKey, type);
            KvPair pair = new KvPair(idType);
            pair.setData(map);
            KeyInfo keyInfo = new KeyInfo();
            if (!AppCtx.getRedisRepo().save(context, pair, keyInfo)) {
                allOk = false;
            }
            Utils.getExcutorService().submit(() -> {
                AppCtx.getDbaseRepo().save(context, pair, keyInfo);
            });
        }
        return allOk;
    }

    @Override
    public Collection<Approval> getApprovals(String userId, String clientId) {

        Context context = getContext();
        String hashKey = clientId + "/" + userId;
        KvIdType idType = new KvIdType(hashKey, type);

        Map<String, Object> map = AppCtx.getCacheOps().getData(idType);
        if (map == null) {
            KvPair pair = new KvPair(idType);
            KeyInfo keyInfo = new KeyInfo();
            if (AppCtx.getRedisRepo().find(context, pair, keyInfo)) {
                map = pair.getData();
            } else if (AppCtx.getDbaseRepo().find(context, pair, keyInfo)) {
                map = pair.getData();
            }
        }
        if (map == null) {
            return null;
        }
        List<Object> list = Utils.convertMapToList(map);
        List<Approval> approvals = new ArrayList<>();
        for (Object object: list) {
            map = (Map<String, Object>) object;
            approvals.add(Utils.toPojo(map, Approval.class));
        }
        return approvals;
    }
}
