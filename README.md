[ ![Download](https://api.bintray.com/packages/cogswell-io/maven/cogs-android-client-sdk/images/download.svg)](https://bintray.com/cogswell-io/maven/cogs-android-client-sdk/_latestVersion)

## Description
The Android SDK for the Cogs real-time message brokering system for Publishing and Subscribing (Pub/Sub) and for Complex Event Processing (CEP.)

## Requirements
* Android 4.0.4+
* Android Studio 1.5.0
* CEP requires a GCM enabled application. You can read more about GCM [here](https://developers.google.com/cloud-messaging/)

# Cogswell Java SDK
This page details general information on the Cogswell Android SDK. For specific example code, see:
* [Pub/Sub Code Samples](API-PUBSUB.md)
* [CEP Code Samples](API-CEP.md)

## [Installation](#build-details)
### Manual
* Follow the Android Studio installation instructions appropriate for your platform. http://developer.android.com/sdk/index.html (If you are on Ubuntu 15.04 or later, the only dependencies you need are lib32stdc++6 and lib32z1. Attempting to install some of the others will result in errors)
* You will need SDK version 22. To get this version, open Android Studio > Preferences > Appearance and Behavior > System Settings > Android SDK > SDK Platforms > Check-mark Android 5.1.1, API level 22 > Ok.  The SDK will be installed.
* Once you have Android Studio installed, you will need to create a local.properties file that points to your SDK root.
* On Mac, at the project root, you can try the following command:
```
echo sdk.dir = /Users/`whoami`/Library/Android/sdk > local.properties
```
* Your installed build tools version must match the version in build.gradle.  Because Android studio does not always you to install an older build tools version, you must update build.gradle to use the version that you have.  You can find your version here: Android Studio > Preferences > Appearance and Behavior > System Settings > Android SDK > SDK Tools > Uncheck Android Build Tools > Click Apply but DO NOT click ok to uninstall the tools > Note the version of the build tools in the pop-up (23.0.3 for example) > Click Cancel.* Update buildToolsVersion to match your build tools version.  For example:
```
buildToolsVersion "23.0.3"
```
* Now you can run either your locally installed gradle or the Gradle Wrapper script (gradlew on Linux and OS X; gradlew.bat on Windows) in order to assemble the SDK: `./gradlew install`
* The .aar file is now installed in your local cache, and can be used for either the example app or your own Android app.
* If you have access, you can publish to bintray using `BINTRAY_API_KEY=******** BINTRAY_USER=cogswell-io ./gradlew bintrayUpload`

If you get and error like the following, make sure you have SDK API level 22 installed:
```
FAILURE: Build failed with an exception.
* What went wrong:
A problem occurred configuring root project 'cogs-android-example-app'.
> failed to find target with hash string 'android-22' in: /Users/{YourUserName}/Library/Android/sdk
```
If you get an error like the following, update your build.gradle file as described above:
```
FAILURE: Build failed with an exception.

* What went wrong:
A problem occurred configuring root project 'cogs-android-example-app'.
> failed to find Build Tools revision 22.0.1
```

## Publishing
In order to publish to jcenter, you will need to create a new local.properties file in the root of the project repository, and populate the `user` and `apiKey` fields with the jcenter login credentials.

## License
Copyright 2017 Aviata Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
