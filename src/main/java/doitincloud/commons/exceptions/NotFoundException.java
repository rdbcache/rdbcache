/**
 * @link http://rdbcache.com/
 * @copyright Copyright (c) 2017-2018 Sam Wen
 * @license http://rdbcache.com/license/
 */

package doitincloud.commons.exceptions;

import doitincloud.commons.helpers.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_FOUND)
public class NotFoundException extends RuntimeException {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotFoundException.class);

    public NotFoundException(String message) {
        super(message);
    }

    public NotFoundException(Context context, String message) {
        super(message);
        if (context != null) {
            context.logTraceMessage(message);
            context.closeMonitor();
            LOGGER.info(HttpStatus.NOT_FOUND + " NOT FOUND " + context.getAction());
        } else {
            LOGGER.info(HttpStatus.NOT_FOUND + " NOT FOUND");
        }
    }
}
