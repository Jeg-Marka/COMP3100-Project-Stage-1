import java.net.Socket;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;

public class CSjobDispatch {
    public static void main(String[] args) {
        try {
            Socket sock = new Socket("127.0.0.1", 50000);

            DataOutputStream output = new DataOutputStream(sock.getOutputStream());
            BufferedReader input = new BufferedReader(new InputStreamReader(sock.getInputStream()));

            boolean exitQ = false;

            output.write(("HELO\n").getBytes());
            output.flush();
            System.out.println("SENT: HELO");

            String rcvd = (String) input.readLine();
            System.out.println("RCVD: " + rcvd);

            // Job Info
            String jobQ = "";
            int jID = 0;

            // server info
            String sSize = "";
            int sCore = 0;
            int nServers = 0;

            String usr = System.getProperty("user.name");
            output.write(("AUTH " + usr + "\n").getBytes());
            output.flush();
            System.out.println("SENT: AUTH for " + usr);

            rcvd = (String) input.readLine();
            System.out.println("RCVD: " + rcvd);

            output.write(("REDY\n").getBytes());
            output.flush();
            System.out.println("SENT: redy");

            rcvd = (String) input.readLine();
            System.out.println("RCVD: " + rcvd);

            String[] job = rcvd.split(" ");

            if (job[0].equals("JOBN")) {
                jID = Integer.parseInt(job[2]);

                output.write(("GETS All\n").getBytes());
                output.flush();
                System.out.println("SENT: GETS All");

                rcvd = (String) input.readLine();
                System.out.println("RCVD: " + rcvd);

                String[] srvData = rcvd.split(" ");
                nServers = Integer.parseInt(srvData[1]);
                System.out.println("No. Of Servers: " + nServers);

                output.write(("OK\n").getBytes());
                output.flush();
                System.out.println("SENT: OK");

                // Determines Maxium Server Size and Count of the server
                int maxCore = 0;
                String maxServer = "Undetermined";
                int msCount = 0;

                for (int i = 0; i < nServers; i++) {
                    rcvd = (String) input.readLine();
                    System.out.println("RCVD: " + rcvd);
                    String[] srvInfo = rcvd.split(" ");
                    sSize = srvInfo[0];
                    sCore = Integer.parseInt(srvInfo[4]);

                    if (sCore > maxCore) {
                        maxServer = sSize;
                        maxCore = sCore;
                        msCount = 0;
                    }
                    msCount++;
                }

                System.out.println("Largest Server: " + maxServer + " Count: " + msCount);

                output.write(("OK\n").getBytes());
                output.flush();
                System.out.println("SENT: OK");

                rcvd = (String) input.readLine();
                System.out.println("RCVD: " + rcvd);
                while (jobQ.equals("NONE") == false) {
                    for (int i = 0; i < msCount && jobQ.equals("NONE") == false; i++) {
                        output.write(("REDY\n").getBytes());
                        output.flush();
                        System.out.println("SENT: redy");

                        rcvd = (String) input.readLine();
                        System.out.println("RCVD: " + rcvd);

                        job = rcvd.split(" ");
                        jobQ = job[0];

                        if (jobQ.equals("JOBN")) {
                            jID = Integer.parseInt(job[2]);

                            output.write(("SCHD " + jID + " " + maxServer + " " + i + "\n").getBytes());
                            output.flush();
                            System.out.println("SCHD: " + jID + " TO: " + maxServer + " " + i);
                            rcvd = (String) input.readLine();
                            System.out.println("RCVD: " + rcvd);
                        } else if (jobQ.equals("JCPL")) {
                            i--;
                        } else if (jobQ.equals("NONE")) {

                            output.write(("QUIT\n").getBytes());
                            output.flush();
                            System.out.println("SENT: QUIT");

                            rcvd = (String) input.readLine();
                            System.out.println("RCVD: " + rcvd);
                            exitQ = true;
                        }
                    }
                }
            }

            if (exitQ = false) {

                output.write(("QUIT\n").getBytes());
                output.flush();
                System.out.println("SENT: QUIT");

                rcvd = (String) input.readLine();
                System.out.println("RCVD: " + rcvd);

                input.close();
                output.close();
                sock.close();

            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
