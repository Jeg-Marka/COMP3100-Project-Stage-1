import java.net.Socket;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;

public class CSjobDispatch {
    public static void main(String[] args) {
        try {
            Socket sock = new Socket("127.0.0.1", 50000); //opens up a conection to the default port for the DS Sim server

            DataOutputStream output = new DataOutputStream(sock.getOutputStream()); //establishes data stream to send information through the socket
            BufferedReader input = new BufferedReader(new InputStreamReader(sock.getInputStream())); //Establishes data reader to read data from the socket

            boolean exitQ = false; // to determine if the program should exit based on reciving a NONE msg

            output.write(("HELO\n").getBytes()); //sends a HELO comand to the server
            output.flush();
            System.out.println("SENT: HELO");

            String rcvd = (String) input.readLine(); // reads msg from the server
            System.out.println("RCVD: " + rcvd);

            // Job Info
            String jobQ = "";
            int jID = 0;

            // server info
            String sSize = "";
            int sCore = 0;
            int nServers = 0;

            String usr = System.getProperty("user.name"); // sends authenitaction msg to the server
            output.write(("AUTH " + usr + "\n").getBytes());
            output.flush();
            System.out.println("SENT: AUTH for " + usr);

            rcvd = (String) input.readLine(); // reads msg from the server
            System.out.println("RCVD: " + rcvd);

            output.write(("REDY\n").getBytes()); // sends REDY msg to the server
            output.flush();
            System.out.println("SENT: redy");

            rcvd = (String) input.readLine(); // reads msg from the server
            System.out.println("RCVD: " + rcvd);

            String[] job = rcvd.split(" "); //splits last msg from the server into a string array for each word

            if (job[0].equals("JOBN")) { //checks to see that the server has sent a JOB 
                jID = Integer.parseInt(job[2]); //grabs the JOB ID from the last server msg

                output.write(("GETS All\n").getBytes()); //sends GETS ALL message to the server to request stored server data
                output.flush();
                System.out.println("SENT: GETS All");

                rcvd = (String) input.readLine();  // reads msg from the server
                System.out.println("RCVD: " + rcvd);

                String[] srvData = rcvd.split(" ");  //splits last msg from the server into a string array for each word
                nServers = Integer.parseInt(srvData[1]); // records how many servers are avaliable on the Simulator from the server data.
                System.out.println("No. Of Servers: " + nServers);

                output.write(("OK\n").getBytes()); // Sends OK msg to the server to request list of servers. 
                output.flush();
                System.out.println("SENT: OK");

                //intialises server information variables
                int maxCore = 0;
                String maxServer = "Undetermined";
                int msCount = 0;

                for (int i = 0; i < nServers; i++) {
                    rcvd = (String) input.readLine(); // reads msg from the server
                    System.out.println("RCVD: " + rcvd); 
                    String[] srvInfo = rcvd.split(" "); //splits last msg from the server into a string array for each word for the server
                    //sets server info from string array
                    sSize = srvInfo[0]; 
                    sCore = Integer.parseInt(srvInfo[4]);

                    if (sCore > maxCore) { // tests to see if previously largest server is larger than current
                        maxServer = sSize; //if current server is larger records new largest server
                        maxCore = sCore;
                        msCount = 0; //sets largest server count to 0
                    }
                    if (sSize.equals(maxServer)) {
                        msCount++; //increases count of the largest server type. 
                    }
                }

                System.out.println("Largest Server: " + maxServer + " Count: " + msCount);

                output.write(("OK\n").getBytes()); // sends ok msg to the srv
                output.flush();
                System.out.println("SENT: OK");

                rcvd = (String) input.readLine(); // reads msg from the server 
                System.out.println("RCVD: " + rcvd);
                while (jobQ.equals("NONE") == false) { //while loop that tests to see if last message from the srv recived was NONE
                    for (int i = 0; i < msCount && jobQ.equals("NONE") == false; i++) { //for loop that continues for the count of max servers and tests to see if last message from the srv recived was NONE
                        output.write(("REDY\n").getBytes()); // sends REDY msg to the server to recive new JOB
                        output.flush();
                        System.out.println("SENT: redy");

                        rcvd = (String) input.readLine(); // reads msg from the server 
                        System.out.println("RCVD: " + rcvd);

                        job = rcvd.split(" ");  //splits last msg from the server into a string array for each word
                        jobQ = job[0]; //sets jobq to inital server msg

                        if (jobQ.equals("JOBN")) { //checks if MSG is a JOB
                            jID = Integer.parseInt(job[2]); //grabs JOB ID from last recicved msg

                            output.write(("SCHD " + jID + " " + maxServer + " " + i + "\n").getBytes()); //builds schedule job command based on JOB ID and Chosen server
                            output.flush();
                            System.out.println("SCHD: " + jID + " TO: " + maxServer + " " + i);
                            rcvd = (String) input.readLine();// reads msg from the server
                            System.out.println("RCVD: " + rcvd);
                        } else if (jobQ.equals("JCPL")) { //checks if MSG is a JOB complete
                            i--;
                        } else if (jobQ.equals("NONE")) { //checks if MSG is a NONE for no more JOBS

                            output.write(("QUIT\n").getBytes()); //Sends QUIT msg to the server to close the connection
                            output.flush();
                            System.out.println("SENT: QUIT");

                            rcvd = (String) input.readLine(); // reads msg from the server
                            System.out.println("RCVD: " + rcvd);
                            exitQ = true; //sets exitq to true to prevent a second exit attempt
                        }
                    }
                }
            }

            if (exitQ = false) { //checks if  server has exited while loop without setting exitQ to true
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
