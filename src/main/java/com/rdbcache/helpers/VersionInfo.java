/*
 * Copyright (c) 2017-2018, Sam Wen <sam underscore wen at yahoo dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of rdbcache nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.rdbcache.helpers;

import java.io.IOException;
import java.net.URL;
import java.net.JarURLConnection;
import java.util.Calendar;
import java.util.Properties;
import java.util.jar.Manifest;
import java.util.jar.Attributes;

public class VersionInfo {

    private String jdkVersion;

    private String bootVersion;

    private String appName;

    private String appVersion;

    private String appDescription;

    private String appUrl;

    private String appAuthors;

    private String appProfile;

    private String appBuildNumber;

    private String appBuildTime;

    private String appBuiltBy;

    private String briefInfo;

    public VersionInfo() {

        boolean loaded = false;

        Properties properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("application.properties"));
            loaded = true;
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (loaded) {
            
            jdkVersion = properties.getProperty("jdk.version");
            bootVersion = properties.getProperty("boot.version");
            appName = properties.getProperty("app.name");
            appVersion = properties.getProperty("app.version");
            appDescription = properties.getProperty("app.description");
            appUrl = properties.getProperty("app.url");
            appAuthors = properties.getProperty("app.authors");
            appProfile = properties.getProperty("app.profile");
            appBuildNumber = properties.getProperty("app.buildNumber");
            appBuildTime = properties.getProperty("app.buildTime");
            appBuiltBy = properties.getProperty("app.builtBy");

            if (loaded && jdkVersion == null) loaded = false;
            if (loaded && bootVersion == null) loaded = false;
            if (loaded && appName == null) loaded = false;
            if (loaded && appVersion == null) loaded = false;
            if (loaded && appDescription == null) loaded = false;
            if (loaded && appUrl == null) loaded = false;
            if (loaded && appAuthors == null) loaded = false;
            if (loaded && appProfile == null) loaded = false;
            if (loaded && appBuildNumber == null) loaded = false;
            if (loaded && appBuildTime == null) loaded = false;
            if (loaded && appBuiltBy == null) loaded = false;
        }
        if (loaded) return;

        Attributes attributes = null;

        try {
            String className = this.getClass().getSimpleName()+".class";
            String classPath = this.getClass().getResource(className).toString();
            URL url = new URL(classPath);
            JarURLConnection jarConnection = (JarURLConnection) url.openConnection();
            Manifest manifest = jarConnection.getManifest();
            attributes = manifest.getMainAttributes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (attributes == null) return;

        if (jdkVersion == null) jdkVersion = attributes.getValue("Build-Jdk");
        if (bootVersion == null) bootVersion = attributes.getValue("Spring-Boot-Version");
        if (appName == null) appName = attributes.getValue("Implementation-Title");
        if (appVersion == null) appVersion = attributes.getValue("Implementation-Version");
        if (appDescription == null) appDescription = attributes.getValue("Description");
        if (appUrl == null) appUrl = attributes.getValue("Url");
        if (appAuthors == null) appAuthors = attributes.getValue("Authors");
        if (appProfile == null) appProfile = attributes.getValue("Pofile-Id");
        if (appBuildNumber == null) appBuildNumber = attributes.getValue("Implementation-Build");
        if (appBuildTime == null) appBuildTime = attributes.getValue("Build-Time");
        if (appBuiltBy == null) appBuiltBy = attributes.getValue("Built-By");
    }

    public String getBriefInfo() {

        if (briefInfo != null) return briefInfo;

        if (appName.equals("default")) {
            return "rdbcache";
        }

        briefInfo = appName + " " +
                appVersion + " rev." + appBuildNumber + " " + appProfile + " @ " +
                appBuildTime + " built by " + appBuiltBy;

        return briefInfo;
    }

    public String getFullInfo() {

        if (appName == null) {
            return "rdbcache";
        }

        String info = getBriefInfo() + "\n";
        info += appDescription + "\n";
        info += "Website: " + appUrl + "\n";
        int year = Calendar.getInstance().get(Calendar.YEAR);
        info += "Copyright (c) 2017-" + year + " " + appAuthors;
        return info;
    }

    public String getJdkVersion() {
        return jdkVersion;
    }

    public String getBootVersion() {
        return bootVersion;
    }

    public String getAppName() {
        return appName;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getAppDescription() {
        return appDescription;
    }

    public String getAppUrl() {
        return appUrl;
    }

    public String getAppAuthors() {
        return appAuthors;
    }

    public String getAppProfile() {
        return appProfile;
    }

    public String getAppBuildNumber() {
        return appBuildNumber;
    }

    public String getAppBuildTime() {
        return appBuildTime;
    }

    public String getAppBuiltBy() {
        return appBuiltBy;
    }
}
