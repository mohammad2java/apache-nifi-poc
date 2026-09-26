## Steps to Build a Java Custom Processor

1. Prerequisites: Install [Java and Maven](https://maven.apache.org/) on your system. [5]
2. Generate Project: Run the Maven archetype command in your terminal: [6]

mvn archetype:generate -DarchetypeGroupId=org.apache.nifi -DarchetypeArtifactId=nifi-processor-bundle-archetype

3. Implement Logic: Open the generated processor class (which extends AbstractProcessor), define your properties and relationships, and write your core logic inside the onTrigger(ProcessContext context, ProcessSession session) method. [1, 4]
4. Build the NAR: Compile the project using mvn clean install to generate a NiFi Archive (.nar) file. [7, 8]
5. Deploy: Copy the compiled .nar file into the lib/ directory of your [Apache NiFi](https://nifi.apache.org/) installation and restart the service. [7, 8]


