package doitincloud.oauth2.repositories;

import java.util.Collection;

public interface TokenRepo<T> {

    public T get(String key);

    public Collection<T> getCollection(String key);

    public String put(String key, T tObject);

    public void put(String key, Collection<T> tObjects);

    public T remove(String key);

    public boolean containsKey(String key);
}
