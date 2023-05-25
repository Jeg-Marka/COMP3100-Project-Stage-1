import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;

public class CSJobDispatchStage2better {

  public static void main(String[] args) {
    try {
      Socket sock = new Socket("127.0.0.1", 50000); //opens up a conection to the default port for the DS Sim server

      DataOutputStream output = new DataOutputStream(sock.getOutputStream()); //establishes data stream to send information through the socket
      BufferedReader input = new BufferedReader(
        new InputStreamReader(sock.getInputStream())
      ); //Establishes data reader to read data from the socket

      boolean exitQ = false; // to determine if the program should exit based on reciving a NONE msg

      output.write(("HELO\n").getBytes()); //sends a HELO comand to the server
      output.flush();
      System.out.println("SENT: HELO");

      String rcvd = (String) input.readLine(); // reads msg from the server
      System.out.println("RCVD: " + rcvd);

      // Job Info
      String jobQ = "";
      int jID = -10;
      int jcore = 0;
      int jsize = 0;
      int jspeed = 0;

      // server info
      int nServers = 0;
      String srvSize = "";
      int sID = 0;
      String srvAct = "";
      int sCore = 0;
      int sSize = 0;
      int sSpeed = 0;
      String[] srv;
      String[] srvData;

      String usr = System.getProperty("user.name"); // sends authenitaction msg to the server
      output.write(("AUTH " + usr + "\n").getBytes());
      output.flush();
      System.out.println("SENT: AUTH for " + usr);

      rcvd = (String) input.readLine(); // reads msg from the server
      System.out.println("RCVD: " + rcvd);

      output.write(("REDY\n").getBytes()); // sends REDY msg to the server
      output.flush();
      System.out.println("SENT: REDY");

      rcvd = (String) input.readLine(); // reads msg from the server
      System.out.println("RCVD: " + rcvd);

      String[] job = rcvd.split(" "); //splits last msg from the server into a string array for each word

      jobQ = job[0]; //sets jobq to inital server msg

        while (jobQ.equals("NONE") == false) { //while loop that tests to see if last message from the srv recived was NONE
            if (job[0].equals("JOBN")) { //checks to see that the server has sent a JOB
                jID = Integer.parseInt(job[2]); //grabs the JOB ID from the last server msg
                jcore = Integer.parseInt(job[4]);
                jsize = Integer.parseInt(job[5]);
                jspeed = Integer.parseInt(job[6]);
              
                
                output.write(("GETS Available " + jcore + " " + jsize + " " + jspeed + "\n").getBytes()); //sends GETS Available message to the server to request stored server data for any servers that are capable and able to do the job
                output.flush();
                System.out.println("SENT: GETS Available " + jcore + " " + jsize + " " + jspeed);
        
                rcvd = (String) input.readLine(); // reads msg from the server
                System.out.println("RCVD: " + rcvd);
        
                srvData = rcvd.split(" "); //splits last msg from the server into a string array for each word
                nServers = Integer.parseInt(srvData[1]); // records how many servers are avaliable on the Simulator from the server data.
                System.out.println("No. Of Servers: " + nServers);

                if (nServers > 0) {
                  //System.out.println("Avalible");
                  output.write(("OK\n").getBytes()); // Sends OK msg to the server to request list of servers.
                  output.flush();
                  System.out.println("SENT: OK");
                  String smallest = "";
                  int srvCount = 0;
                  int wJobs = 100000;
                  boolean sStarted = false;
                  for (int i = 0; i < nServers; i++) {
                    rcvd = (String) input.readLine(); // reads msg from the server
                    System.out.println("RCVD: " + rcvd);
                    srv = rcvd.split(" "); //splits last msg from the server into a string array for each word for the server
                    //server information variables
                    
                    String srvSizec = srv[0];
                    int sIDc = Integer.parseInt(srv[1]);
                    String srvActc = srv[2];
                    int srvStartc = Integer.parseInt(srv[3]);
                    int sCorec = Integer.parseInt(srv[4]);
                    int sSizec = Integer.parseInt(srv[5]);
                    int sSpeedc = Integer.parseInt(srv[6]);
                    int wJobsc = Integer.parseInt(srv[7]);
                    //small     10      inactive    -1  2       6000    16000   0   0
                    //srvSize   sID     srvAct      ??  sCore   sSize   sSpeed  wJobs
                    // 0        1       2           3   4       5       6       7   8 
                    
                    if (i == 0){ 
                      smallest = srvSizec;
                    }

                    if (smallest.equals(srvSizec) && srvStartc > 0) {
                      srvSize = srv[0];
                      sID = Integer.parseInt(srv[1]);
                      srvAct = srv[2];
                      sCore = Integer.parseInt(srv[4]);
                      sSize = Integer.parseInt(srv[5]);
                      sSpeed = Integer.parseInt(srv[6]);
                      wJobs = Integer.parseInt(srv[7]);
                      sStarted =true;
                    } 

                    if (smallest.equals(srvSizec) && sStarted == false) {
                      srvSize = srv[0];
                      sID = Integer.parseInt(srv[1]);
                      srvAct = srv[2];
                      sCore = Integer.parseInt(srv[4]);
                      sSize = Integer.parseInt(srv[5]);
                      sSpeed = Integer.parseInt(srv[6]);
                      wJobs = Integer.parseInt(srv[7]);
                    } 
                  //System.out.println("RCVD: " + rcvd);
                  //System.out.println(i);
                  }
                //System.out.println("Prints alot of servers");
                } else if (nServers == 0) { //if there are no servers available see what servers are capable.
                  output.write(("OK\n").getBytes()); // Sends OK msg to the server to request list of servers.
                  output.flush();
                  System.out.println("SENT: OK");

                  rcvd = (String) input.readLine(); // reads msg from the server
                    System.out.println("RCVD: " + rcvd);

                  output.write(("GETS Capable " + jcore + " " + jsize + " " + jspeed + "\n").getBytes()); //sends GETS ALL message to the server to request stored server data
                  output.flush();
                  System.out.println("SENT: GETS Capable " + jcore + " " + jsize + " " + jspeed);
        
                  rcvd = (String) input.readLine(); // reads msg from the server
                  System.out.println("RCVD: " + rcvd);
                  
                  srvData = rcvd.split(" "); //splits last msg from the server into a string array for each word
                  nServers = Integer.parseInt(srvData[1]); // records how many servers are avaliable on the Simulator from the server data.
                  System.out.println("No. Of Servers: " + nServers);

                  //start
                  output.write(("OK\n").getBytes()); // Sends OK msg to the server to request list of servers.
                  output.flush();
                  System.out.println("SENT: OK");
                  String smallest = "";
                  int srvCount = 0;
                  int wJobs = 100000;
                  for (int i = 0; i < nServers; i++) {
                    rcvd = (String) input.readLine(); // reads msg from the server
                    System.out.println("RCVD: " + rcvd);
                    srv = rcvd.split(" "); //splits last msg from the server into a string array for each word for the server
                    //server information variables
                    
                    String srvSizec = srv[0];
                    int sIDc = Integer.parseInt(srv[1]);
                    String srvActc = srv[2];
                    int sCorec = Integer.parseInt(srv[4]);
                    int sSizec = Integer.parseInt(srv[5]);
                    int sSpeedc = Integer.parseInt(srv[6]);
                    int wJobsc = Integer.parseInt(srv[7]);
                    //small     10      inactive    -1  2       6000    16000   0   0
                    //srvSize   sID     srvAct      ??  sCore   sSize   sSpeed  wJobs
                    // 0        1       2           3   4       5       6       7   8 
                    
                    if (i == 0){ 
                      smallest = srvSizec;
                    }

                    if (wJobsc < wJobs && smallest.equals(srvSizec)) {
                      srvSize = srv[0];
                      sID = Integer.parseInt(srv[1]);
                      srvAct = srv[2];
                      sCore = Integer.parseInt(srv[4]);
                      sSize = Integer.parseInt(srv[5]);
                      sSpeed = Integer.parseInt(srv[6]);
                      wJobs = Integer.parseInt(srv[7]);
                    } 


                  } 
                  //System.out.println("RCVD: " + rcvd);
                  //System.out.println(i);
                }
                //System.out.println("Prints alot of servers");
                
        
              
        
                output.write(("OK\n").getBytes()); // sends ok msg to the srv
                output.flush();
                System.out.println("SENT: OK");
        
                rcvd = (String) input.readLine(); // reads msg from the server
                //System.out.println("RCVD: " + rcvd);

                output.write(("SCHD " + jID + " " + srvSize + " " + sID + "\n").getBytes()); //builds schedule job command based on JOB ID and Chosen server
                output.flush();
                System.out.println("SCHD: " + jID + " TO: " + srvSize + " " + sID);
                rcvd = (String) input.readLine();// reads msg from the server
                System.out.println("RCVD: " + rcvd);
          
          } else if (jobQ.equals("JCPL")) { //checks if MSG is a JOB complete
            //nothing
          } else if (jobQ.equals("NONE")) { //checks if MSG is a NONE for no more JOBS
            output.write(("QUIT\n").getBytes()); //Sends QUIT msg to the server to close the connection
            output.flush();
            System.out.println("SENT: QUIT");

            rcvd = (String) input.readLine(); // reads msg from the server
            System.out.println("RCVD: " + rcvd);
            exitQ = true; //sets exitq to true to prevent a second exit attempt
          }

          if (exitQ == false) {
            output.write(("REDY\n").getBytes()); // sends REDY msg to the server to recive new JOB
            output.flush();
            System.out.println("SENT: REDY");

            rcvd = (String) input.readLine(); // reads msg from the server
            System.out.println("RCVD: " + rcvd);

            if (rcvd != null & !rcvd.isEmpty()){
                job = rcvd.split(" ");  //splits last msg from the server into a string array for each word
                jobQ = job[0]; //sets jobq to inital server msg
            }
          }
        }
      

      if (exitQ == false) { //checks if  server has exited while loop without setting exitQ to true
        //this is for redundancy would only be nesacary if the for loop exited and the next msg from the server was NONE
        output.write(("QUIT\n").getBytes()); //Sends QUIT command to the server
        output.flush();
        System.out.println("SENT: QUIT");

        rcvd = (String) input.readLine(); // reads msg from the server
        System.out.println("RCVD: " + rcvd);

        input.close(); //closes Input reader
        output.close(); // closes output data stream
        sock.close(); // closes connection to the socket
      }
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
