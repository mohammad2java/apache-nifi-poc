## APACHE NIFI notes - 2.12.0
-----------------------------
    APACHE NIFI is a powerful data integration tool that allows for the automation of data flow between systems. 
    It provides a web-based interface for designing, monitoring, and managing data flows.
    
    core components of APACHE NIFI include:
    1. **Processors**: These are the building blocks of data flows. Each processor performs actions
       2. connections --connector between 2 processor.
       3. FlowFile  --actual data from between processor or in connection queue.
       3. process group --combine flow diagram in single entity, similar like folder for files.
       4. template in 1.x / flow definition in 2.x --resuable export/import file xml/json for designed flow.
       5. custom processor -- processor for custom task..that are not availble from official kit.
    
    
    
    
    
    Quick start:
-------------------

      1) download zip .
      2) setup JAVA_HOME
      3) nifi start  like C:\Users\HP\Downloads\nifi-2.12.0-bin\nifi-2.12.0\bin>nifi start
      4) url : https://localhost:8443/nifi/
      5) nifi stop --for stopping

--------------------
        for login user/password
        
        open logs/nifi-app.log and search "Generated Username"
        you will get username/password both..













