package com.bigdata;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class App {
    static JFrame frame = new JFrame("BLM4821 - Term Project");
    private JTextArea logArea;
    private JButton connectButton;
    private JLabel statusLabel;
    private JPanel panelMain;
    private JComboBox inputComboBox;
    private JComboBox functionComboBox;
    private JTextField outputTextField;
    private JButton uploadButton;
    private JButton downloadButton;
    private JButton runButton;

    private static boolean connectionFlag = false;
    Configuration configuration;
    FileSystem fileSystem;
    public App() throws IOException {
        configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        fileSystem = FileSystem.get(configuration);
        logArea.setEditable(false);

        functionComboBox.addItem("Select a function");
        functionComboBox.addItem("Count");
        functionComboBox.addItem("Average");
        functionComboBox.addItem("Min-Max");
        functionComboBox.addItem("Median");
        functionComboBox.addItem("Standard Deviation");

        connectButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                try {
                    Process process = Runtime.getRuntime().exec("start-all.sh");
                    StringBuilder output = new StringBuilder();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    String line;
                    while ((line = reader.readLine()) != null)
                        output.append(line + "\n");
                    int exitVal = process.waitFor();
                    //hadoop dfsadmin
                    process = Runtime.getRuntime().exec("hdfs dfsadmin -safemode leave");
                    output = new StringBuilder();
                    reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                    while ((line = reader.readLine()) != null)
                        output.append(line + "\n");
                    exitVal = process.waitFor();

                    if (exitVal == 0) {
                        connectionFlag = true;
                        statusLabel.setText("Status: Connected");
                        logArea.append("Connection succesful on 'localhost:9870'.\n");
                        connectButton.setEnabled(false);
                        Path getFilesFromPath = new Path("/BigData/Input/");
                        FileStatus statuses[] = fileSystem.listStatus(getFilesFromPath);
                        for (FileStatus status : statuses) {
                            //System.out.println("Path : " + status.getPath().getName());
                            inputComboBox.addItem(status.getPath().getName());
                        }
                        System.out.println("Success!");
                        //System.out.println(output);
                    } else {
                        System.out.println("An error occurred.");
                    }
                }
                catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        uploadButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                JFileChooser fileChooser = new JFileChooser();
                int option = fileChooser.showOpenDialog(frame);
                if(option == JFileChooser.APPROVE_OPTION){
                    File file = fileChooser.getSelectedFile();
                    String fileName = file.getName();
                    Path filepath = new Path(file.getAbsolutePath());
                    Path hdfsWritePath = new Path("/BigData/Input/" + fileName);

                    logArea.append("File Selected: " + fileName+"\n");
                    logArea.append("'" + fileName + "' is being uploaded to HDFS.\n");
                    try {
                        fileSystem.copyFromLocalFile(filepath, hdfsWritePath);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    Path getFilesFromPath = new Path("/BigData/Input/");
                    FileStatus statuses[];
                    try {
                        statuses = fileSystem.listStatus(getFilesFromPath);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    inputComboBox.removeAllItems();
                    for (FileStatus status : statuses)
                        inputComboBox.addItem(status.getPath().getName());

                }else{
                    logArea.append("*Open command canceled.\n");
                }
            }
        });
        downloadButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                String userDir = System.getProperty("user.home");
                JFileChooser fileChooser = new JFileChooser(userDir);
                fileChooser.setDialogTitle("Download");
                fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
                fileChooser.setAcceptAllFileFilterUsed(false);
                //
                if (fileChooser.showOpenDialog(frame) == JFileChooser.APPROVE_OPTION) {
                    String input_file = (String) inputComboBox.getSelectedItem();
                    Path getFilesFromPath = new Path("/BigData/Input/" + input_file);
                    Path localPath = new Path(fileChooser.getSelectedFile().toString());
                    try {
                        fileSystem.copyToLocalFile(false, getFilesFromPath, localPath);
                        String log = "'" + input_file + "' has been downloaded to '" + localPath.toString() + "'";
                        logArea.append(log+"\n");

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                else {
                    System.out.println("No Selection ");
                }
            }
        });
        runButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                int selection = functionComboBox.getSelectedIndex();
                String input_file = (String) inputComboBox.getSelectedItem();
                String outputDirectory = outputTextField.getText();
                String fileName = "part-r-00000";

                switch (selection) {
                    case 0:
                        JOptionPane.showMessageDialog(frame, "You did not select any functions!");
                        break;
                    case 1:
                        if(outputDirectory.equals(""))
                            JOptionPane.showMessageDialog(frame, "Empty!");
                        else{
                            try {
                                long start = System.currentTimeMillis();
                                Process process = Runtime.getRuntime().exec("hadoop jar /home/hadoopuser/IdeaProjects/BigData/Count/Count.jar Count /BigData/Input/" + input_file + " /Count/" + outputTextField.getText());
                                System.out.println("Process in progress.");
                                process.waitFor();
                                long end = System.currentTimeMillis();
                                float sec = (end - start) / 1000F;
                                System.out.println(sec + " seconds");

                                Path hdfsReadPath = new Path("/Count/" + outputDirectory + "/" + fileName);
                                FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                                String out= IOUtils.toString(inputStream, "UTF-8");
                                System.out.println(out);
                                logArea.setText("Count in file '" + input_file + "':\n");
                                logArea.append(out+"\n");
                                inputStream.close();
                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case 2:
                        if(outputDirectory.equals(""))
                            JOptionPane.showMessageDialog(frame, "Empty!");
                        else{
                            try {
                                long start = System.currentTimeMillis();
                                Process process = Runtime.getRuntime().exec("hadoop jar /home/hadoopuser/IdeaProjects/BigData/Average/Average.jar Average /BigData/Input/" + input_file + " /Average/" + outputTextField.getText());
                                System.out.println("Process in progress.");
                                process.waitFor();
                                long end = System.currentTimeMillis();
                                float sec = (end - start) / 1000F;
                                System.out.println(sec + " seconds");

                                Path hdfsReadPath = new Path("/Average/" + outputDirectory + "/" + fileName);
                                FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                                String out= IOUtils.toString(inputStream, "UTF-8");
                                System.out.println(out);
                                logArea.setText("Average in file '" + input_file + "':\n");
                                logArea.append(out+"\n");
                                inputStream.close();
                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case 3:
                        if(outputDirectory.equals(""))
                            JOptionPane.showMessageDialog(frame, "Empty!");
                        else{
                            try {
                                long start = System.currentTimeMillis();
                                Process process = Runtime.getRuntime().exec("hadoop jar /home/hadoopuser/IdeaProjects/BigData/MinMax/MinMax.jar MinMax /BigData/Input/" + input_file + " /MinMax/" + outputTextField.getText());
                                System.out.println("Process in progress.");
                                process.waitFor();
                                long end = System.currentTimeMillis();
                                float sec = (end - start) / 1000F;
                                System.out.println(sec + " seconds");

                                Path hdfsReadPath = new Path("/MinMax/" + outputDirectory + "/" + fileName);
                                FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                                String out= IOUtils.toString(inputStream, "UTF-8");
                                System.out.println(out);
                                logArea.setText("MinMax in file '" + input_file + "':\n");
                                logArea.append(out+"\n");
                                inputStream.close();
                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case 4:
                        if(outputDirectory.equals(""))
                            JOptionPane.showMessageDialog(frame, "Empty!");
                        else{
                            try {
                                long start = System.currentTimeMillis();
                                Process process = Runtime.getRuntime().exec("hadoop jar /home/hadoopuser/IdeaProjects/BigData/Median/Median.jar Median /BigData/Input/" + input_file + " /Median/" + outputTextField.getText());
                                System.out.println("Process in progress.");
                                process.waitFor();
                                long end = System.currentTimeMillis();
                                float sec = (end - start) / 1000F;
                                System.out.println(sec + " seconds");

                                Path hdfsReadPath = new Path("/Median/" + outputDirectory + "/" + fileName);
                                FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                                String out= IOUtils.toString(inputStream, "UTF-8");
                                System.out.println(out);
                                logArea.setText("Median in file '" + input_file + "':\n");
                                logArea.append(out+"\n");
                                inputStream.close();
                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    case 5:
                        if(outputDirectory.equals(""))
                            JOptionPane.showMessageDialog(frame, "Empty!");
                        else{
                            try {
                                long start = System.currentTimeMillis();
                                Process process = Runtime.getRuntime().exec("hadoop jar /home/hadoopuser/IdeaProjects/BigData/StandardDeviation/StandardDeviation.jar StandardDeviation /BigData/Input/" + input_file + " /StandardDeviation/" + outputTextField.getText());
                                System.out.println("Process in progress.");
                                process.waitFor();
                                long end = System.currentTimeMillis();
                                float sec = (end - start) / 1000F;
                                System.out.println(sec + " seconds");

                                Path hdfsReadPath = new Path("/StandardDeviation/" + outputDirectory + "/" + fileName);
                                FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                                String out= IOUtils.toString(inputStream, "UTF-8");
                                System.out.println(out);
                                logArea.setText("StandardDeviation in file '" + input_file + "':\n");
                                logArea.append(out+"\n");
                                inputStream.close();
                            }
                            catch (Exception ex) {
                                ex.printStackTrace();
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        });
    }

    public static void main(String[] args) throws IOException{
        frame.setContentPane(new App().panelMain);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(600,600);
        frame.setVisible(true);
        frame.addWindowListener(new java.awt.event.WindowAdapter() {
            @Override
            public void windowClosing(java.awt.event.WindowEvent windowEvent) {
                if(connectionFlag){
                    try {
                        Process process = Runtime.getRuntime().exec("stop-dfs.sh");
                        StringBuilder output = new StringBuilder();
                        BufferedReader reader = new BufferedReader(
                                new InputStreamReader(process.getInputStream()));
                        String line;
                        while ((line = reader.readLine()) != null)
                            output.append(line + "\n");
                        int exitVal = process.waitFor();
                        if (exitVal == 0) {
                            connectionFlag = false;
                            System.out.println("Success!");
                            System.out.println(output);
                        }
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.exit(0);
            }
        });
    }

}
