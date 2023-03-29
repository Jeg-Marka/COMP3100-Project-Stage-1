import java.net.Socket;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class CSjobDispatchOP {
    static String Message(String msg, DataOutputStream output, BufferedReader input ) {
        String rcvd = "";
        
        try {
            output.write((msg+"\n").getBytes());
            output.flush();
            System.out.println("SENT: "+ msg);

            rcvd = (String) input.readLine();
            System.out.println("RCVD: " + rcvd);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rcvd;
    }
    public static void main(String[] args) {
        try {
            Socket sock = new Socket("127.0.0.1", 50000);
            DataOutputStream output = new DataOutputStream(sock.getOutputStream());
            BufferedReader input = new BufferedReader(new InputStreamReader(sock.getInputStream()));
            boolean exitQ = false;
            Message("HELO", output, input);

            // Job Info
            String jobQ = "";
            int jID = 0;

            // server info
            String sSize = "";
            int sCore = 0;
            int nServers = 0;

            String usr = System.getProperty("user.name");
            Message("AUTH " + usr, output, input);

            String[] job = Message("redy", output, input).split(" ");

            if (job[0].equals("JOBN")) {
                jID = Integer.parseInt(job[2]);

                String[] srvData = Message("GETS All", output, input).split(" ");
                nServers = Integer.parseInt(srvData[1]);
                System.out.println("No. Of Servers: " + nServers);

                String reply = Message("OK", output, input);

                // Determines Maxium Server Size and Count of the server
                int maxCore = 0;
                String maxServer = "Undetermined";
                int msCount = 0;

                for (int i = 0; i < nServers; i++) {
                    reply = (String) input.readLine();
                    System.out.println("RCVD: " + reply);
                    String[] srvInfo = reply.split(" ");
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
                Message("OK", output, input);

                while (jobQ.equals("NONE") == false) {
                    for (int i = 0; i < msCount && jobQ.equals("NONE") == false; i++) {
                        job = Message("redy", output, input).split(" ");
                        jobQ = job[0];

                        if (jobQ.equals("JOBN")) {
                            jID = Integer.parseInt(job[2]);
                            Message("SCHD " + jID + " " + maxServer + " " + i, output, input);
                        } else if (jobQ.equals("JCPL")) {
                            i--;
                        } else if (jobQ.equals("NONE")) {
                            Message("OK", output, input)
                            Message("QUIT", output, input);
                            exitQ = true;
                        }
                    }
                }
            }

            if (exitQ = false) {
                Message("OK", output, input);
                Message("QUIT", output, input);

                input.close();
                output.close();
                sock.close();

            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}