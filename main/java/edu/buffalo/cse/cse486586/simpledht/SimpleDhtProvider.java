package edu.buffalo.cse.cse486586.simpledht;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.ContentValues.TAG;

public class SimpleDhtProvider extends ContentProvider {

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		File path = SimpleDhtActivity.context.getFilesDir(); // check
		File file = new File(path, "data.properties");
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Properties prop = new Properties();
		InputStream is = null;
		String key = selection;

		try {
			is = new FileInputStream(file);
			prop.load(is);
			if (key.equals("*") || key.equals("@")) {
				for (Object k : prop.keySet()) {
					String key1 = (String) k;
					prop.remove(key);
					OutputStream out = new FileOutputStream(file);
					prop.store(out, null);
				}
			} else {
				prop.remove(key);
				OutputStream out = new FileOutputStream(file);
				prop.store(out, null);
			}
			return 1;
		} catch (Exception e) {
			e.printStackTrace();
		}

		Log.v("query", selection);
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = values.getAsString("key");
		String value = values.getAsString("value");
		Log.v("Key TO INSERT", key);
		boolean thisAvd = false;

		try {
			if (genHash(key).compareTo(curr.hashName) < 0 && genHash(key).compareTo(prev.hashName) > 0)
				thisAvd = true;

			if (!thisAvd && avds.size() == 1)
				thisAvd = true;

			if (curr.hashName.compareTo(prev.hashName) < 0
					&& (genHash(key).compareTo(curr.hashName) < 0 || genHash(key).compareTo(prev.hashName) > 0)) {
				thisAvd = true;
			}
			// Log.v("INFO", " @"+prev.hashName+"@ @"+curr.hashName+"@ @"+next.hashName+"@
			// @"+genHash(key)+"@");
			if (thisAvd) {
				// Log.v("INSERT", "own");
				File path = SimpleDhtActivity.context.getFilesDir();
				File file = new File(path, "data.properties");
				// File file = new
				// File("/data/data/edu.buffalo.cse.cse486586.groupmessenger1/files/data.properties");

				if (!file.exists())
					file.createNewFile();
				Properties prop = new Properties();
				InputStream is = null;
				is = new FileInputStream(file);
				prop.load(is);
				prop.setProperty(String.valueOf(key), value);
				OutputStream out = new FileOutputStream(file);
				prop.store(out, "added :|");
				// Log.v("insert1", values.toString());
			} else {
				String msg = "Insert\n" + key + "\n" + value;
				// Log.v("INSERT", "at other avd");
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, String.valueOf(next.portNumber));
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return uri;
	}

	static class HashAvd implements Comparable<HashAvd> {
		String hashName;
		int portNumber;

		HashAvd(String name, int port) {
			hashName = name;
			portNumber = port;
		}

		@Override
		public int compareTo(HashAvd another) {
			return hashName.compareTo(another.hashName);
		}
	}

	static int SERVER_PORT = 10000;
	static int MYPORT, ENDPORT;
	static HashAvd prev, next, curr;
	static TreeSet<HashAvd> avds = new TreeSet<HashAvd>();
	// static HashMap<String, HashAvd> getHashAvd = new HashMap<String, HashAvd>();

	public static void setPrevCurr() {
		if (avds.lower(curr) == null)
			prev = avds.last();
		else
			prev = avds.lower(curr);

		if (avds.higher(curr) == null)
			next = avds.first();
		else
			next = avds.higher(curr);
	}

	public static Uri getUri() {
		String uriii = "edu.buffalo.cse.cse486586.simpledht.provider";
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(uriii);
		uriBuilder.scheme("content");
		Uri uri = uriBuilder.build();
		return uri;
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		// starting point
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		MYPORT = Integer.parseInt(myPort);
		ENDPORT = MYPORT;
		// Log.v("PORT","the port is "+myPort+" portstr "+portStr);

		try {
			curr = new HashAvd(genHash(portStr), Integer.parseInt(myPort));
			avds.add(curr);
			setPrevCurr();
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			if (!portStr.equals("5554")) {
				String msg = "Alive:1\n" + curr.hashName + "\n" + myPort;
				// Thread.sleep(2000);
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, "11108");
			}
		} catch (NoSuchAlgorithmException e) {
			Log.e("ALGO FAILED", " No such algo exception");
		} catch (IOException e) {
			Log.e("IOException", "fault fault");
		}
		return false;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {

			try {
				ServerSocket serverSocket = sockets[0];
				// int ServerPort = ports[0];
				Scanner in = null;
				PrintWriter writer = null;
				// Log.v("SERVER","Server started..Port:"+MYPORT+"##");

				while (true) {
					//
					Socket sock = null;
					sock = serverSocket.accept();
					in = new Scanner(sock.getInputStream());
					writer = new PrintWriter(sock.getOutputStream(), true);
					String cmd = in.nextLine();
					if (cmd.contains("Alive")) { // adding new clients to ring
						int port = 0;
						int n = Integer.parseInt(cmd.split(":")[1]);
						for (int i = 0; i < n; i++) {
							String name = in.nextLine();
							port = Integer.parseInt(in.nextLine());
							HashAvd avd = new HashAvd(name, port);
							avds.add(avd);
						}
						if (MYPORT == 11108) {
							StringBuilder msg = new StringBuilder();
							msg.append("Alive:" + avds.size() + "\n");
							for (HashAvd a : avds) {
								msg.append(a.hashName + "\n" + a.portNumber + "\n");
							}
							for (HashAvd a : avds) {
								if (a.portNumber == 11108)
									continue;
								new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(),
										String.valueOf(a.portNumber));
							}
						}
						// Log.v("CURRENT","Current is +"+curr.hashName+" "+curr.portNumber);
						setPrevCurr();
						for (HashAvd avd : avds) {
							Log.v("Diplay" + avds.size(), "name " + avd.hashName + " port" + avd.portNumber);
						}
					} // end adding nodes
					else if (cmd.contains("Insert")) {
						// Log.v("INSERT AVD", "Msg sent from another avd to insert");
						String key1 = in.nextLine();
						String value1 = in.nextLine();
						ContentValues keyValue = new ContentValues();
						keyValue.put("key", key1);
						keyValue.put("value", value1);
						SimpleDhtActivity.mContentResolver.insert(getUri(), keyValue);
					} else if (cmd.contains("Query")) {
						if (cmd.contains("*")) {
							ENDPORT = Integer.parseInt(cmd.split(":")[1]);
							Log.v("INFO", "Endport is " + ENDPORT);
							Log.v("INFO", "cmd is:" + cmd);
						}
						if (!cmd.contains(String.valueOf(MYPORT))) {
							String key = in.nextLine();
							/*
							 * if(key.equals("*")) key="**";
							 */
							Log.v("INFO", "QUERY REACHED, key:" + key);
							Cursor resultCursor = SimpleDhtActivity.mContentResolver.query(getUri(), null, key, null,
									null);
							StringBuilder msg = new StringBuilder();
							if (resultCursor == null || resultCursor.getCount() < 1) { // intentionally sent
								msg.append("empty");
							} else {
								writer = new PrintWriter(sock.getOutputStream(), true);
								resultCursor.moveToFirst();
								while (resultCursor.isAfterLast() == false) {
									String key1 = resultCursor.getString(resultCursor.getColumnIndex("key"));
									String value1 = resultCursor.getString(resultCursor.getColumnIndex("value"));
									msg.append(key1 + "\n" + value1 + "\n");
									resultCursor.moveToNext();
								}
							}
							Log.v("INFO", "message " + msg.toString());
							writer.println(msg.toString());
							writer.flush();
						} else {
							Log.v("INFO", "Has REached the start node, sending stop");
							writer = new PrintWriter(sock.getOutputStream(), true);
							writer.println("stop");
							writer.flush();
						}
					}
					writer.close();
					in.close();
				} // while ends
			} catch (IOException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... strings) {

			String msg = strings[0];
			String port = strings[1];
			// Log.v("Client","Sending message "+msg+" Port:"+port);
			try {
				Socket sock = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), Integer.parseInt(port));
				PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
				writer.println(msg);
				writer.flush();
				// sock.close();

			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		MatrixCursor cursor = new MatrixCursor(new String[] { "key", "value" });
		File path = SimpleDhtActivity.context.getFilesDir(); // check
		File file = new File(path, "data.properties");
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Properties prop = new Properties();
		InputStream is = null;
		String key = selection;

		try {
			is = new FileInputStream(file);
			prop.load(is);
			if (key.equals("@")) {
				for (Object k : prop.keySet()) {
					String key1 = (String) k;
					String val1 = prop.getProperty(key1);
					cursor.addRow(new String[] { key1, val1 });
					Log.v("sending All", key1 + "  " + val1);
				}
			} else if (selection.contains("*")) {
				if (selection.equals("*"))
					ENDPORT = MYPORT;
				for (Object k : prop.keySet()) {
					String key1 = (String) k;
					String val1 = prop.getProperty(key1);
					cursor.addRow(new String[] { key1, val1 });
					Log.v("STORED", key1 + "  " + val1);
				}
				Log.v(String.valueOf(MYPORT), "SELECTION key " + key);
				String value = "";
				HashAvd avd = next;
				Log.v("INFO", "Sending to node with port " + avd.portNumber);
				String msg = "Query:" + ENDPORT + ":**" + "\n" + "**";
				Socket sock = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), avd.portNumber);
				PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
				writer.println(msg);
				writer.flush();
				Scanner in = new Scanner(sock.getInputStream());
				String key1 = in.nextLine();
				if (key1.contains("empty")) {
					Log.v("INFO", "Empty returned");
				} else if (!key1.contains("stop")) {
					value = in.nextLine();
					cursor.addRow(new String[] { key1, value });
					while (in.hasNext()) {
						key1 = in.nextLine();
						value = in.nextLine();
						cursor.addRow(new String[] { key1, value });
					}
				} else {
					Log.v("INFO", "STOP Cndition met");
				}
				in.close();
				sock.close();
			} else {
				String val = prop.getProperty(key);
				if (val == null) {
					// Log.v("INFO", "key "+key);
					String value = "";
					HashAvd avd = next;
					// Log.v("INFO","Sending to node with port "+avd.portNumber);
					String msg = "Query:" + MYPORT + "\n" + key;
					Socket sock = new Socket(InetAddress.getByAddress(new byte[] { 10, 0, 2, 2 }), avd.portNumber);
					PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
					writer.println(msg);
					writer.flush();
					Scanner in = new Scanner(sock.getInputStream());
					in.nextLine();
					value = in.nextLine();
					in.close();
					sock.close();
					cursor.addRow(new String[] { key, value });
				} else {
					Log.v("CHECKK", key + "  " + val);
					cursor.addRow(new String[] { key, val });
				}
			}
			return cursor;
		} catch (Exception e) {
			e.printStackTrace();
		}
		Log.v("query", selection);
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
