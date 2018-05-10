/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.exceptions;

import doitincloud.rdbcache.supports.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
public class ServerErrorException extends RuntimeException {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerErrorException.class);

    public ServerErrorException(String message) {
        super(message);
    }

    public ServerErrorException(Context context, String message) {
        super(message);
        if (context != null) {
            context.logTraceMessage(message);
            context.closeMonitor();
            LOGGER.info(HttpStatus.INTERNAL_SERVER_ERROR + " INTERNAL SERVER ERROR " + context.getAction());
        } else {
            LOGGER.info(HttpStatus.INTERNAL_SERVER_ERROR + " INTERNAL SERVER ERROR");
        }
    }
}
