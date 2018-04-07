package doitincloud.commons.supports;

import doitincloud.commons.supports.PasswordMatches;
import doitincloud.security.forms.SignupInfo;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

public class PasswordMatchesValidator implements ConstraintValidator<PasswordMatches, Object> {

    @Override
    public void initialize(PasswordMatches constraintAnnotation) {
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        SignupInfo user = (SignupInfo) value;
        return user.getPassword().equals(user.getConfirmPassword());
    }
}
