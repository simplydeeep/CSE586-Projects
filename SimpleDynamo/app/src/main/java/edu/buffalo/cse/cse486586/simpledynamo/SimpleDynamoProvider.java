package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.File;
import java.io.FileOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.MatrixCursor;
import android.database.Cursor;
import android.net.Uri;

import android.util.Log;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

public class SimpleDynamoProvider extends ContentProvider {

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int SERVER_PORT = 10000;

	private final String fNode = "5554";
	private final String Join = "J";
	private final String Update = "U";
	private final String Insert = "I";
	private final String InsertR = "IR";
	private final String Delete = "D";
	private final String DeleteR = "DR";
	private final String DeleteAllGlobal = "DG";
	private final String Query = "Q";
	private final String QueryR = "R";
	private final String QueryAllGlobal = "QG";
	private final String QueryReplica = "QR";

	private String mNode;
	private String pNode = null;
	private String sNode = null;
	private String qResponse;
	private String gResponse;
	private String rResponse;
	private boolean flag = true;
	private boolean replicaFlag = true;
	private boolean iqFlag = false;
	private HashMap<String, String> localMap = new HashMap<String, String>();

	private String node2;
	private String node3;
	private String node4;
	private String node5;
	private String[] nodes = {"5562", "5556", "5554", "5558", "5560"};

	@Override
	synchronized public Uri insert(Uri uri, ContentValues values) {
		String key1 = (String) values.get("key");
		String value1 = (String) values.get("value");
		Log.e("Insert Called by Script", mNode + ":" + key1 + ":" + value1);
		String inMessage = key1 + ":" + value1;
		insert(inMessage);
		return uri;
	}

	public void insert(String message) {
		String fName = message.split(":")[0];
		String fContent = message.split(":")[1];

		String partNode = getPartitionNode(fName);

//		if (partNode.equals(mNode)) {
//			Log.e("Insert", mNode + ":" + fName + ":" + fContent);
//			localMap.put(fName, fContent);
//			String iR2 = InsertR+":"+node2+":"+fName+":"+fContent;
//			clientThread(iR2);
//		} else {
//			String iMsg = InsertR+":"+partNode+":"+fName+":"+fContent;
//			clientThread(iMsg);
//		}

		String iPort = null;
		String iPort2 = null;
		String iPort3 = null;

		if (nodes[3].equals(partNode)) {
			iPort = nodes[3];
			iPort2 = nodes[4];
			iPort3 = nodes[0];
		}
		else if (nodes[4].equals(partNode)) {
			iPort = nodes[4];
			iPort2 = nodes[0];
			iPort3 = nodes[1];
		} else {
			for (int i=0; i<nodes.length-2; i++) {
				if (nodes[i].equals(partNode)) {
					iPort = nodes[i];
					iPort2 = nodes[i+1];
					iPort3 = nodes[i+2];
					break;
				}
			}
		}

		if (iPort.equals(mNode)) {
			Log.e("Insert", mNode+":"+fName+":"+fContent);
			localMap.put(fName, fContent);
			String iR2 = InsertR+":"+iPort2+":"+fName+":"+fContent;
			clientThread(iR2);
			String iR3 = InsertR+":"+iPort3+":"+fName+":"+fContent;
			clientThread(iR3);
		} else if (iPort2.equals(mNode)) {
			Log.e("Insert", mNode+":"+fName+":"+fContent);
			localMap.put(fName, fContent);
			String iR = InsertR+":"+iPort+":"+fName+":"+fContent;
			clientThread(iR);
			String iR3 = InsertR+":"+iPort3+":"+fName+":"+fContent;
			clientThread(iR3);

		} else if (iPort3.equals(mNode)) {
			Log.e("Insert", mNode+":"+fName+":"+fContent);
			localMap.put(fName, fContent);
			String iR = InsertR+":"+iPort+":"+fName+":"+fContent;
			clientThread(iR);
			String iR2 = InsertR+":"+iPort2+":"+fName+":"+fContent;
			clientThread(iR2);
		}
		else {
		String iR = InsertR+":"+iPort+":"+fName+":"+fContent;
		clientThread(iR);
		String iR2 = InsertR+":"+iPort2+":"+fName+":"+fContent;
		clientThread(iR2);
		String iR3 = InsertR+":"+iPort3+":"+fName+":"+fContent;
		clientThread(iR3);
		}
	}

	public void insertR(String message) {
		String fName = message.split(":")[0];
		String fContent = message.split(":")[1];
		localMap.put(fName, fContent);
	}

	@Override
	synchronized public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
									 String sortOrder) {
		Log.w("Query Called by Script", selection);
		String port = mNode;
		if (selection.equals("*")) {
			String g = QueryAllGlobal+":"+port+":"+port+":"+"T";
			return queryAllGlobal(g);
		}
		else if (selection.equals("@")) {
			return queryAllLocal();
		}
		else {
			MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});
			String partNode = getPartitionNode(selection);

			if (partNode.equals(mNode)) {
				MatrixCursor.RowBuilder mRB = matCursor.newRow();
				mRB.add("key", selection);
				mRB.add("value", localMap.get(selection));
				Log.w("Query From Self Reply", mNode +":" + selection +":" + localMap.get(selection));
				return matCursor;
			} else {
				String qR = QueryR + ":" + partNode + ":" + selection + ":" + "ScriptElse" + ":"+ port+":"+"T";
				return queryR(qR);
			}
		}
	}

	public Cursor queryR (String message) {
		String port = message.split(":")[4];
		String selection = message.split(":")[2];
		String iPort = message.split(":")[1];

		MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

		String partNode = getPartitionNode(selection);

		if (mNode.equals(iPort)) {
			if (localMap.get(selection) != null) {
				String qR = QueryR + ":" + port + ":" + selection + ":" + localMap.get(selection) + ":" + port + ":" + "T";
				clientThread(qR);
			} else {
				String qR = QueryR + ":" + partNode + ":" + selection + ":" + "PtaNahi" + ":" + port + ":" + "T";
				clientThread(qR);

			}
		} else {
			String qR = QueryR + ":" + iPort + ":" + selection + ":" + "ScriptSeAayaHua" + ":"+ port+":"+"T";
			clientThread(qR);
		}

		if (message.split(":")[5].equals("T")) {
			while (flag) { }
			MatrixCursor.RowBuilder mRB = matCursor.newRow();
			mRB.add("key", selection);
			mRB.add("value", qResponse);
			Log.w("Query From Other Reply", mNode +":" + selection +":" + qResponse);
			flag = true;
			return matCursor;
		}
		return matCursor;
	}

	public Cursor queryAllLocal() {
		MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

		for (Map.Entry<String, String> entries: localMap.entrySet()) {
			MatrixCursor.RowBuilder mRB = matCursor.newRow();
			mRB.add("key", entries.getKey());
			mRB.add("value", entries.getValue());
		}
		return matCursor;
	}

	public Cursor queryAllGlobal(String message) {
		MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

		for (Map.Entry<String, String> entries : localMap.entrySet()) {
			gResponse += ":"+entries.getKey()+"__"+entries.getValue();
		}
		String gMsg = message.substring(0,3) + sNode + message.substring(7)+":"+gResponse;
		clientThread(gMsg);

		if (message.split(":")[3].equals("T")) {
			while (flag) { }
			String[] fullResponse = gResponse.split(":");
			int i=0;
			while (i<fullResponse.length) {
				if (!fullResponse[i].equals("null")) {
					String key1 = fullResponse[i].split("__")[0];
					String val1 = fullResponse[i].split("__")[1];
					MatrixCursor.RowBuilder mRB = matCursor.newRow();
					mRB.add("key", key1);
					mRB.add("value", val1);
				}
				i++;
			}
			gResponse = null;
			flag = true;
		}
//		}
		return matCursor;
	}

	@Override
	synchronized public int delete(Uri uri, String selection, String[] selectionArgs) {
		String port = mNode;
		if (selection.equals("*")) {
			String d = DeleteAllGlobal+":"+port+":"+port;
			deleteAllGlobal(d);
		}
		else if (selection.equals("@")) {
			deleteAllLocal();
		}
		else {
			String d = Delete+":"+port+":"+selection+":"+port;
			delete(d);
		}
		return 0;
	}

	public void delete (String message) {
		String port = message.split(":")[3];
		String selection = message.split(":")[2];
		try {
			String hName = genHash(selection);
			if ((pNode == null && sNode == null)
					|| (hName.compareTo(genHash(pNode)) > 0 && hName.compareTo(genHash(mNode)) <= 0)
					|| (genHash(pNode).compareTo(genHash(mNode)) > 0 && (hName.compareTo(genHash(pNode)) > 0
					|| hName.compareTo(genHash(mNode)) <= 0))) {
				localMap.remove(selection);
				String dR2 = DeleteR+":"+node2+":"+selection;
				clientThread(dR2);
				String dR3 = DeleteR+":"+node3+":"+selection;
				clientThread(dR3);
			} else {
				String dMsg = Delete + ":" + sNode + ":" + selection+":"+port;
				clientThread(dMsg);
			}
		} catch (NoSuchAlgorithmException e) { Log.e("Query", "Hashing problem"); }
	}

	public void deleteR(String selection) { localMap.remove(selection); }

	public void deleteAllLocal() { localMap.clear(); }

	public void deleteAllGlobal(String message) {
		localMap.clear();
		String port = message.split(":")[2];
		if (!sNode.equals(port)) {
			String gMsg = DeleteAllGlobal+":"+sNode+":"+port;
			clientThread(gMsg);
		}
	}

	public Cursor queryReplica(String message) {
		Log.d("Query Replica Function", mNode);
		MatrixCursor matCursor = new MatrixCursor(new String[]{"key", "value"});

		for (Map.Entry<String, String> entries : localMap.entrySet()) {
			rResponse += ":" + entries.getKey() + "__" + entries.getValue();
		}

		String rMsg = message.substring(0,3) + node2 + message.substring(7)+":"+rResponse;
		clientThread(rMsg);

		if (message.split(":")[3].equals("T")) {
			while (replicaFlag) { }
			Log.d("Query Replica MapPut", mNode);
			String[] fullResponse = rResponse.split(":");
			int i=0;
			while (i<fullResponse.length) {
				if (!fullResponse[i].equals("null") && fullResponse[i].split("__").length==2) {
					String key1 = fullResponse[i].split("__")[0];
					String val1 = fullResponse[i].split("__")[1];

					String port = getPartitionNode(key1);
					if (port.equals(mNode) || port.equals(node4) || port.equals(node5))
						localMap.put(key1, val1);
				}
				i++;
			}
			rResponse = null;
			replicaFlag = true;
		}
		rResponse = null;
		return matCursor;
	}

	@Override
	public boolean onCreate() {
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		mNode = portStr;
		dhtNodes(mNode);

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			return false;
		}

		String name = "check";
		File checkFile = new File(this.getContext().getFilesDir(), name);
		if(checkFile.exists()) {
			pNode = node5;
			sNode = node2;
			String r = QueryReplica + ":" + mNode + ":" + mNode + ":" + "T";
			Log.d("Query Replica OnCreate", mNode);
			queryReplica(r);
		}
		else{
			try {
				FileOutputStream fos;
				Context mContext = this.getContext();
				fos = mContext.openFileOutput(name, Context.MODE_PRIVATE);
				fos.write(name.getBytes());
				fos.close();
				if (!mNode.equals(fNode)) {
					String msg = Join+":"+fNode+":"+mNode;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
				}
			}
			catch(Exception e){ }
		}
		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			try {
				while (true) {
					Socket socket = serverSocket.accept();
					BufferedReader bRD = new BufferedReader(new InputStreamReader(socket.getInputStream()));
					PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
					String temp = bRD.readLine();

					if (temp != null) {
						out.println("ACK");
						Log.e("Server Task", temp);
						String[] msgReceived = temp.split(":");

						if (msgReceived[0].equals(Join)) {

							if (pNode == null && sNode == null) {
								sNode = msgReceived[2];
								pNode = msgReceived[2];
								String updateP = Update + ":" +msgReceived[2] + ":" + msgReceived[1] + ":" + msgReceived[1];
								clientThread(updateP);

							} else {
								try {
									String hashedKey = genHash(msgReceived[2]);
									if ((genHash(pNode).compareTo(genHash(msgReceived[1])) > 0 && hashedKey.compareTo(genHash(pNode)) > 0)
											|| (genHash(pNode).compareTo(genHash(msgReceived[1])) > 0 && hashedKey.compareTo(genHash(msgReceived[1])) <= 0)
											|| (hashedKey.compareTo(genHash(pNode)) > 0 && hashedKey.compareTo(genHash(msgReceived[1])) <= 0)) {

										String updateS = Update + ":" + pNode + ":" + null + ":" + msgReceived[2];
										clientThread(updateS);

										String update = Update + ":" + msgReceived[2] + ":" + pNode + ":" + msgReceived[1];
										clientThread(update);

										pNode = msgReceived[2];
									} else {
										String msg = Join + ":" + sNode + ":" + msgReceived[2];
										clientThread(msg);
									}
								} catch (NoSuchAlgorithmException e) { }
							}
						}
						else if (msgReceived[0].equals(Update)) {
							if (!msgReceived[2].equals("null")) {
								pNode = msgReceived[2];
							}
							if (!msgReceived[3].equals("null")) {
								sNode = msgReceived[3];
							}
						}
						else if (msgReceived[0].equals(Insert)) {
							insert(msgReceived[2]+":"+msgReceived[3]);
						}
//						else if (msgReceived[0].equals(Query)) {
////							Log.e("Server Query", "My Node: "+mNode+ " Port: "+msgReceived[4]);
//							if (mNode.equals(msgReceived[4])) {
//								qResponse = msgReceived[3];
//								Log.e("Server Query Original", "My Node: "+mNode+ " qResponse: "+qResponse);
//								flag = false;
//							} else {
//								String q = msgReceived[0]+":"+msgReceived[1]+":"+msgReceived[2]+":"+msgReceived[3]+":"+msgReceived[4]+":"+"F"+":"+"FF";
//								Log.e("Server Query Else ", "My Node: "+mNode+ " Message: "+q);
//								query(q);
//							}
//						}
						else if (msgReceived[0].equals(QueryR)) {
							if (mNode.equals(msgReceived[4])) {
								qResponse = msgReceived[3];
								flag = false;
							} else {
								String q = msgReceived[0]+":"+msgReceived[1]+":"+msgReceived[2]+":"+"ServerKa"+":"+msgReceived[4]+":"+"F";
								queryR(q);
							}
						}
						else if (msgReceived[0].equals(QueryAllGlobal)) {
							if (mNode.equals(msgReceived[2])) {
								gResponse = temp.substring(15);
								flag = false;
							} else {
								String g = temp.substring(0,13) + "F" + temp.substring(14);
								queryAllGlobal(g);
							}
						}
						else if (msgReceived[0].equals(Delete)) {
							delete(temp);
						}
						else if (msgReceived[0].equals(DeleteAllGlobal)) {
							deleteAllGlobal(temp);
						}
						else if (msgReceived[0].equals(InsertR)) {
							insertR(msgReceived[2] +":" + msgReceived[3]);
						}
						else if (msgReceived[0].equals(DeleteR)) {
							deleteR(msgReceived[2]);
						}
						else if (msgReceived[0].equals(QueryReplica)) {
							if (mNode.equals(msgReceived[2])) {
								rResponse = temp.substring(15);
								replicaFlag = false;
							} else {
								String r = temp.substring(0,13) + "F" + temp.substring(14);
								queryReplica(r);
							}
						}
					}
					socket.close();
				}
			} catch (IOException e) { Log.e(TAG, "ServerTask Exception"); }
			return null;
		}

		protected void onProgressUpdate(String...strings) { }
	}

	public void clientThread(String message) {
		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {
		@Override
		protected Void doInBackground(String... msgs) {
			String[] msgReceived = msgs[0].split(":");
			int port = (Integer.parseInt(msgReceived[1]))*2;
			Socket socket = null;
			BufferedReader bRD = null;
			try {
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
				String msgToSend = msgs[0];
				PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
				Log.e("Client Task", msgToSend);
				out.println(msgToSend);
				bRD = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				while (true) {
					String temp = bRD.readLine();
					if (temp.equals("ACK")) {
						break;
					}
				}
			} catch (Exception e) {
//				if (msgReceived[0].equals(InsertR)) {
//					Log.e("Insert @ Exception", "Failed Node: " +msgReceived[1]);
//
//					String selection = msgReceived[2];
//					String iPort2 = null;
//					String iPort3 = null;
//
//					String partNode = getPartitionNode(selection);
//
//					if (nodes[3].equals(partNode)) {
//						iPort2 = nodes[4];
//						iPort3 = nodes[0];
//					} else if (nodes[4].equals(partNode)) {
//						iPort2 = nodes[0];
//						iPort3 = nodes[1];
//					} else {
//						for (int i = 0; i < nodes.length - 2; i++) {
//							if (nodes[i].equals(partNode)) {
//								iPort2 = nodes[i + 1];
//								iPort3 = nodes[i + 2];
//								break;
//							}
//						}
//					}
//
//					if (msgReceived[1].equals(partNode)) {
//						if (iPort2.equals(mNode)) {
//							localMap.put(msgReceived[2], msgReceived[3]);
//						} else {
//							String iR = InsertR + ":" + iPort2 + ":" + msgReceived[2] + ":" + msgReceived[3];
//							clientThread(iR);
//						}
//					} else if (msgReceived[1].equals(iPort2)) {
//						if (iPort3.equals(mNode)) {
//							localMap.put(msgReceived[2], msgReceived[3]);
//						} else {
//							String iR = InsertR + ":" + iPort3 + ":" + msgReceived[2] + ":" + msgReceived[3];
//							clientThread(iR);
//						}
//					}
//				}

				if (msgReceived[0].equals(QueryR)) {
					Log.e("Query @ Exception", "Failed Node: " +msgReceived[1] + " key was: "+ msgReceived[2]);

					String selection = msgReceived[2];
					String iPort2 = null;

					String partNode = getPartitionNode(selection);

					if (nodes[3].equals(partNode)) {
						iPort2 = nodes[4];
					} else if (nodes[4].equals(partNode)) {
						iPort2 = nodes[0];
					} else {
						for (int i = 0; i < nodes.length - 2; i++) {
							if (nodes[i].equals(partNode)) {
								iPort2 = nodes[i + 1];
								break;
							}
						}
					}

					String qR = QueryR + ":" + iPort2 + ":" + msgReceived[2] + ":" + localMap.get(selection) + ":"+ msgReceived[4]+":"+"T";
					clientThread(qR);
				}

				if (msgReceived[0].equals(QueryAllGlobal)) {
					String nextPort = null;
					if (msgReceived[1].equals("5562")) {
						nextPort = "5556";
					} else if (msgReceived[1].equals("5556")) {
						nextPort = "5554";
					} else if (msgReceived[1].equals("5554")) {
						nextPort = "5558";
					} else if (msgReceived[1].equals("5558")) {
						nextPort = "5560";
					} else if (msgReceived[1].equals("5560")) {
						nextPort = "5562";
					}
					String qG = msgs[0].substring(0,3) + nextPort + msgs[0].substring(7);
					clientThread(qG);
				}

				if (msgReceived[0].equals(QueryReplica)) {
					Log.e("Replica @ Exception", "Failed Node: " +msgReceived[1]);

					String nextPort = null;
					if (msgReceived[1].equals("5562")) {
						nextPort = "5556";
					} else if (msgReceived[1].equals("5556")) {
						nextPort = "5554";
					} else if (msgReceived[1].equals("5554")) {
						nextPort = "5558";
					} else if (msgReceived[1].equals("5558")) {
						nextPort = "5560";
					} else if (msgReceived[1].equals("5560")) {
						nextPort = "5562";
					}
					String qrep = msgs[0].substring(0,3) + nextPort + msgs[0].substring(7);
					clientThread(qrep);
				}
			}
			try {
				bRD.close();
				socket.close();
			} catch (Exception e) {
				Log.e(TAG, "Socket close Exception");
			}
			return null;
		}
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

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void dhtNodes(String node) {
		if (node.equals("5554")) {
			node2 = "5558";
			node3 = "5560";
			node4 = "5562";
			node5 = "5556";
		}
		else if (node.equals("5558")) {
			node2 = "5560";
			node3 = "5562";
			node4 = "5556";
			node5 = "5554";
		}
		else if (node.equals("5560")) {
			node2 = "5562";
			node3 = "5556";
			node4 = "5554";
			node5 = "5558";
		}
		else if (node.equals("5562")) {
			node2 = "5556";
			node3 = "5554";
			node4 = "5558";
			node5 = "5560";
		}
		else if (node.equals("5556")) {
			node2 = "5554";
			node3 = "5558";
			node4 = "5560";
			node5 = "5562";
		}
	}

	public String getPartitionNode(String key) {
		try {
			String hashedKey = genHash(key);
			for (int i=0; i<nodes.length; i++) {
				if (hashedKey.compareTo(genHash(nodes[i])) <= 0) {
					return nodes[i];
				}
			}
			if (hashedKey.compareTo(genHash(nodes[4])) > 0) {
				return nodes[0];
			}
		} catch (NoSuchAlgorithmException e) { }
		return null;
	}
}
