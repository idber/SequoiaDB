#SequoiaDB Java Driver#

We use Maven to compile, test and package.

See http://maven.apache.org.

##Compile##

Run command:

```
mvn clean compile
```

##Test##

You should make sure that SequoiaDB servers are running, and set a profile for Maven.

For example:

```
<profile>
    <id>dev</id>
    <properties>
        <single.host>localhost</single.host>
        <single.port>11810</single.port>
        <single.username> </single.username>
        <single.password> </single.password>
        <cluster.urls>localhost:11810</cluster.urls>
        <cluster.username> </cluster.username>
        <cluster.password> </cluster.password>
    </properties>
</profile>
```

The profile can be set in maven setting.xml file in your home path(recommended) or pom.xml, 

Then run command:

```
mvn clean test -Pdev
```

You can specify which testcase to run by using -Dtest=<testcase>:

```
mvn clean test -Pdev -Dtest=Test*
```

See maven-surefire-plugin document for more details.

##Package##

Run command:

```
mvn clean package
```

If you want to skip test when package, run command:
```
mvn clean package -Dmaven.test.skip=true
```

If you also want to generate sourcecode and javadoc package, run command:

```
mvn clean package -Prelease
```