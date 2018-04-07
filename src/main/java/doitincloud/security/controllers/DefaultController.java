package doitincloud.security.controllers;

import doitincloud.security.forms.LoginInfo;
import doitincloud.security.forms.SignupInfo;
import doitincloud.security.models.User;
import doitincloud.security.repositories.RememberMeTokenRepo;
import doitincloud.security.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.RememberMeAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;

@Controller
public class DefaultController {

    @Autowired
    private UserService userService;

    @Autowired
    private RememberMeTokenRepo rememberMeTokenRepo;

    @GetMapping("/")
    public String home1() {
        return "home";
    }

    @GetMapping("/home")
    public String home() {
        return "home";
    }

    @GetMapping("/admin")
    public String admin() {
        return "admin";
    }

    @GetMapping("/user")
    public String user() {
        return "user";
    }

    @GetMapping("/about")
    public String about() {
        return "about";
    }

    /*
    @GetMapping("/login")
    public String login() {
        return "login";
    }
    */

    @RequestMapping(value = "/login", method = RequestMethod.GET)
    public String showLogin(WebRequest request, Model model) {
        LoginInfo loginInfo = new LoginInfo();
        String username = SecurityContextHolder.getContext().getAuthentication().getName();
        if (username != null && !username.equals("anonymousUser")) {
            loginInfo.setUsername(username);
            String seriesId = RequestContextHolder.currentRequestAttributes().getSessionId();
            if (seriesId != null) {
                if (rememberMeTokenRepo.getTokenForSeries(seriesId) != null) {
                    loginInfo.setRememberMe(true);
                }
            }
        }
        model.addAttribute("loginInfo", loginInfo);
        return "login";
    }

    @RequestMapping(value = "/signup", method = RequestMethod.GET)
    public String showSignupForm(WebRequest request, Model model) {
        SignupInfo signupInfo = new SignupInfo();
        model.addAttribute("signupInfo", signupInfo);
        model.addAttribute("mapRoles", userService.getAvailableRoles());
        return "signup";
    }

    @RequestMapping(value = "/signup", method = RequestMethod.POST)
    public ModelAndView signupUser(@ModelAttribute("signupInfo") @Valid SignupInfo signupInfo,
                             BindingResult result, WebRequest request, Errors errors) {
        if (result.hasErrors()) {
            return new ModelAndView("signup", "signupInfo", signupInfo);
        }
        User newUser = userService.createNewUser(signupInfo);
        if (newUser == null) {
            result.rejectValue("email", "message.regError", "email address is used");
        }
        if (result.hasErrors()) {
            return new ModelAndView("signup", "signupInfo", signupInfo);
        }
        return new ModelAndView("signup_ok", "user", newUser);
    }

    @GetMapping("/403")
    public String error403() {
        return "error/403";
    }

    /**
     * Check if user is login by remember me cookie, refer
     * org.springframework.security.authentication.AuthenticationTrustResolverImpl
     */
    private boolean isRememberMeAuthenticated() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null) {
            return false;
        }
        return RememberMeAuthenticationToken.class.isAssignableFrom(authentication.getClass());
    }

    /**
     * save targetURL in session
     */
    private void setRememberMeTargetUrlToSession(HttpServletRequest request){
        HttpSession session = request.getSession(false);
        if(session!=null){
            session.setAttribute("targetUrl", "/admin/update");
        }
    }

    /**
     * get targetURL from session
     */
    private String getRememberMeTargetUrlFromSession(HttpServletRequest request){
        String targetUrl = "";
        HttpSession session = request.getSession(false);
        if(session!=null){
            targetUrl = session.getAttribute("targetUrl")==null?""
                    :session.getAttribute("targetUrl").toString();
        }
        return targetUrl;
    }
}
