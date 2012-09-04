package de.tu_berlin.dima.aim3.querysuggestion.livesuggest;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Functions to interact with the file system or read and write files.
 * 
 * @author Michael Huelfenhaus
 * 
 */
public class IOUtils {

  /**
   * List files with certain file type.
   * 
   * @param _dirName
   *            Name of the folder that hold the files
   * @param _fileExtention
   *            File type without "."
   * @return Files in directory with given file extension
   * @throws IOException
   */
  public static File[] listTypFiles(String _dirName, String _fileExtention)
      throws IOException {
    final String fileEnd = "." + _fileExtention;
    File dir = new File(_dirName);
    // TODO add check with dir.exists() and raise error if not
    if (dir.exists()) {
      return dir.listFiles(new FilenameFilter() {
        public boolean accept(File _dir, String _filename) {
          return _filename.endsWith(fileEnd);
        }
      });
    } else {
      throw new IOException("Directory doesn't exit: " + _dirName);
    }
  }

  /**
   * List all subdirectories.
   * 
   * @param _dirName
   *            path of directory
   * @return the subdirectories
   */
  public static File[] listFolders(String _dirName) {
    File dir = new File(_dirName);

    // This filter only returns directories
    return dir.listFiles(new FileFilter() {
      public boolean accept(File _file) {
        return _file.isDirectory();
      }
    });
  }

  /**
   * Read a file line by line with given encoding.
   * 
   * @param _file
   *            File that is read
   * @param _encoding
   *            Encoding of file that is read
   * @return List of lines
   */
  public static List<String> readLines(File _file, String _encoding) {
    String line = "";
    List<String> data = new ArrayList<String>();

    BufferedReader br;
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(
          _file), _encoding));

      while ((line = br.readLine()) != null) {

        data.add(line.trim());
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return data;
  }

  /**
   * Read a Utf-8 file line by line.
   * 
   * @param _file
   *            File that is read
   * @return List of lines
   */
  public static List<String> readLinesUtf8(File _file) {
    return readLines(_file, "Utf-8");
  }

  /**
   * Get a Reader for a file with Utf-8 encoding.
   * 
   * @param _file
   *            path of the file
   * @return reader for the file
   */
  public static BufferedReader getUtf8FileReader(File _file) {

    BufferedReader br = null;

    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(
          _file), "Utf-8"));
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return br;
  }

  /**
   * Get a file writer that uses utf-8 encoding.
   * 
   * @param _path
   *            Path of file file that should be written
   * @param _append
   *            toggle if file is overwritten or text is appended
   * @return a writer
   */
  public static BufferedWriter getUtf8FileWriter(String _path, boolean _append) {
    BufferedWriter out = null;
    try {
      out = new BufferedWriter(new OutputStreamWriter(
          new FileOutputStream(_path, _append), "UTF8"));

    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (FileNotFoundException e) {
      System.err.println(_path + " File for writing not found");
      System.exit(1);

    }
    return out;
  }

  /**
   * Create an empty file, if file with this name exists it is overwritten.
   * 
   * @param _path
   *            Path to of the new file.
   */
  public static void createEmptyFile(String _path) {
    BufferedWriter writer = getUtf8FileWriter(_path, false);
    try {
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
      System.err.println(_path + " File for writing not found");
    }
  }
}

