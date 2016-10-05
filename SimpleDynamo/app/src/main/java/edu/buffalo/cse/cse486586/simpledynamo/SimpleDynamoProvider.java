package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

	private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	// Ports

	public int myPort;
	//    public String myPortHash=null;
	public int myPred;
	public int mySucc;
	private static final String key = "key";
	private static final String value = "value";
	public volatile ConcurrentHashMap<String,String> h=new ConcurrentHashMap<String,String> ();
	public ArrayList<Integer> arr;
	public static volatile ConcurrentHashMap<String,String> res=new ConcurrentHashMap<String,String> ();
	public static volatile MatrixCursor qcursor = new MatrixCursor(new String[]{key,value});
	boolean squery=true;
	int aquery=0;
	int ins=0;
	boolean insert=true;
	int rec=0;
	boolean recover=true;
	boolean second=false;
	boolean test=false;
	int check_for=0;
	boolean sem=true;
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		delete(selection);
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
		while(insert==false || squery==false||recover==false );
		insert=false;squery=false;recover=false;

//		Message msg = new Message();
//		msg.type = "check_for";
//		msg.origin = myPort;
//		for(int j:arr){
//			if(j!=myPort)
//				client(msg,j*2);
//		}
//		while(check_for<4);
//		check_for=0;

		insert(values.getAsString(key), values.getAsString(value));

		insert=true;squery=true;recover=true;
//		msg = new Message();
//		msg.type = "check_for_return";
//		msg.origin = myPort;
//		for(int j:arr){
//			if(j!=myPort)
//				client(msg,j*2);
//		}
		return uri;
	}
	private void insert(String key, String value) {
		ins=0;
		try {
			String hash = genHash(key);

			for(int i=0;i<5;i++){
				String mhash=genHash(String.valueOf(arr.get(i)));
				if(i==0) {
					if (hash.compareTo(mhash) <= 0 || ((hash).compareTo(genHash(String.valueOf(arr.get(4)))) > 0 && hash.compareTo(mhash) > 0)){
						Message msg=new Message();
						msg.key=key;
						msg.value=value;
						msg.type="insert_put";
						msg.myPort=0;
						msg.origin=myPort;
						client(msg,arr.get(i)*2);
//						while(insert==false){}
//						insert=false;
						while(ins<1){}

						client(msg,arr.get(i+1)*2);
//						while(insert==false){}
//						insert=false;
						while(ins<2){}
						client(msg,arr.get(i+2)*2);
						while(ins<3){}
						ins=0;
						break;
					}
				}
				else if(hash.compareTo(mhash) <= 0 && hash.compareTo(genHash(String.valueOf(arr.get(i-1))))>0){
					Message msg=new Message();
					msg.key=key;
					msg.value=value;
					msg.type="insert_put";
					msg.myPort=i;
					msg.origin=myPort;
					client(msg,arr.get(i)*2);
//					while(insert==false){}
//					insert=false;
					while(ins<1){}
					client(msg, arr.get((i + 1)%5) * 2);
//						while(insert==false){}
//						insert=false;
					while(ins<2){}
					client(msg, arr.get((i + 2)%5) * 2);
//						while(insert==false){}
//						insert=false;
					while(ins<3){}
					ins=0;
					break;
				}
			}

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, e.getMessage());
		}
	}

	@Override
	public boolean onCreate() {
		recover=false;
		rec=0;
		TelephonyManager tel = (TelephonyManager)this.getContext().getSystemService(
				Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = Integer.parseInt(portStr);
		arr=new ArrayList<Integer>();
		arr.add(5562);
		arr.add(5556);
		arr.add(5554);
		arr.add(5558);
		arr.add(5560);
		Log.i(TAG, myPort+"");
		int i=arr.indexOf(myPort);
		if (i == 0) {
			myPred = arr.get(arr.size() - 1);
			mySucc = arr.get(i + 1);
		} else if (i == arr.size() - 1) {
				myPred = arr.get(i - 1);
				mySucc = (arr.get(0));
		} else {
			myPred = arr.get(i - 1);
			mySucc = arr.get(i + 1);
		}
//        try {
//            myPortHash = genHash(String.valueOf(myPort));
//        }
//        catch(NoSuchAlgorithmException e){
//            Log.d(TAG, "NoSuchAlgo");
//        }
		try {
			ServerSocket serverSocket = new ServerSocket(10000);
			server(serverSocket);
		} catch (IOException e) {
			Log.e(TAG, "ServerSocket:\n" + e.getMessage());
			return false;
		}
//		myPred=myPort;
//		mySucc=myPort;
//		Message msg =new Message();
//		msg.myPort=myPort;
//		msg.pred=myPred;
//		msg.succ=mySucc;
//		msg.h=h;
//		msg.type="join";
//
//		if(myPort!=5554)
//			client(msg, 11108);
//		// TODO Auto-generated method stub
//		try {
//                Thread.sleep(1000);
//            }
//            catch(Exception e){
//                Log.d(TAG,e.toString());
//            }
		Message m = new Message();
		m.origin = myPort;
		m.type = "test";
		client(m, myPred * 2);
		while(test==false);
		test=false;
		if(second) {
//			Message m1 = new Message();
//			m1.type = "check_for";
//			m1.origin = myPort;
//			for(int j:arr){
//				if(j!=myPort)
//					client(m1,j*2);
//			}

			Message msg = new Message();

			msg.type = "recover";
			msg.origin = myPort;
			msg.arr = new ArrayList<Integer>();
			msg.arr.add(arr.get((i - 1 + 5) % 5));
			msg.arr.add(arr.get(i));
			msg.arr.add(arr.get((i - 2 + 5) % 5));

			client(msg, arr.get((i - 1 + 5) % 5) * 2);
//		client(msg,arr.get((i-2+5)%5)*2);
			client(msg, arr.get((i + 1) % 5) * 2);


			while (rec < 2) ;
			rec = 0;

//			m1 = new Message();
//			m1.type = "check_for_return";
//			m1.origin = myPort;
//			for(int j:arr){
//				if(j!=myPort)
//					client(m1,j*2);
//			}

		}
		second=false;
		recover=true;

		return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {

		while(insert==false || squery==false || recover==false );
		Log.d(TAG, "Yes");
		insert=false;squery=false;recover=false;sem=true;
//		Message msg = new Message();
//		msg.type = "check_for";
//		msg.origin = myPort;
//		for(int j:arr){
//			if(j!=myPort)
//				client(msg,j*2);
//		}
//		Message msg = new Message();
//		msg.type = "check_for";
//		msg.origin = myPort;
//		for(int j:arr){
//			if(j!=myPort)
//				client(msg,j*2);
//		}
//		while(check_for<4);
//		check_for=0;
		if (selection.equals("*") || selection.equals("@")) {
			return Allquery(selection, myPort);
		} else {
			Log.d(TAG,"here1");

			return singlequery(selection, myPort);
		}
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
	public void client(final Message msg,final int port){
		new Thread(new Runnable() {


			public void run() {


				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
					output.writeObject(msg);
					output.close();
					socket.close();
				}
//				catch (UnknownHostException e) {
//					Log.e(TAG, "UnknownHostException");
//				}
				catch (Exception e) {
					if(msg.type.equals("query_get")){
						client(msg,msg.pred*2);
					}
					else if(msg.type.equals("insert_put")){
						ins++;
					}
					else if(msg.type.equals("send_client")){
						aquery++;
					}
					else if(msg.type.equals("test")){
						test=true;
					}
					else if(msg.type.equals("check_for")){
						Log.e(TAG, "Check"+port+" "+e.toString());
						check_for++;
					}

					Log.e(TAG, "IOException"+port+msg.type+" "+e.toString());
				}

			}
		}).start();
	}

	private void server(final ServerSocket s) {
		new Thread(new Runnable() {
			public void run() {
				try {
					ServerSocket serverSocket = s;
					while (true) {
						Socket socket = serverSocket.accept();
						ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
						Message msg = (Message) input.readObject();
						try {


							if (msg.type.equals("insert")) {

								insert(msg.key, msg.value);
							}
							else if(msg.type.equals("insert_put")){
								h.put(msg.key,msg.value);
								msg.type="insert_get";
								client(msg, msg.origin * 2);
//								Message m=new Message();
//								m.key=msg.key;
//								m.value=msg.value;
//								m.type="insert_put";
//								m.origin=msg.origin+1;
//								if(m.origin==4) {
//									Log.d(TAG,"Here");
//									insert=true;
//									return;
//								}
//								if(msg.myPort==4) {
//									m.myPort = 0;
//									client(m, arr.get(0) * 2);
//
//								}
//								else{
//									m.myPort=msg.myPort+1;
//									client(m, arr.get(msg.myPort + 1) * 2);
//								}

							}
							else if(msg.type.equals("insert_get")){
								ins++;
							}

							else if(msg.type.equals("send_client")){
								if(msg.key.equals("AllQuery")) {
									Message m = new Message();
									m.key="AllQuery";
									m.h = h;
									m.type = "receive";
									client(m, msg.origin * 2);
								}
								else if(h.containsKey(msg.key)){

									Message m = new Message();
									m.key=msg.key;
									m.value = h.get(msg.key);
									m.type = "receive";
									client(m, msg.origin * 2);
								}
							}
							else if(msg.type.equals("receive")){

								if(msg.key.equals("AllQuery")) {
									aquery++;
									res.putAll(msg.h);
								}
								else {
									MatrixCursor cursor = new MatrixCursor(new String[]{key, value});
									cursor.addRow(new String[]{msg.key, msg.value});
									Log.d(TAG, msg.key + " " + myPort);
									qcursor=cursor;
									squery=true;
								}

							}
							else if(msg.type.equals("query_get")){
								Log.d(TAG, " Ye Rahi");
								while(!h.containsKey(msg.key)){
//									try {
//										Thread.sleep(500);
//									}
//									catch(Exception e){
//										Log.d(TAG,e.toString());
//									}
								}

								Message m = new Message();
								m.key=msg.key;
								m.value = h.get(msg.key);
								m.type = "query_return";
								client(m, msg.origin * 2);

							}
							else if(msg.type.equals("query_return")){
								MatrixCursor cursor = new MatrixCursor(new String[]{key, value});
								cursor.addRow(new String[]{msg.key, (msg.value)});
								qcursor=cursor;
								squery=true;
							}
							else if(msg.type.equals("delete")){
								if(msg.key.equals("notcheck")){
									h.clear();
								}
								else if(h.containsKey(msg.key)) h.remove(msg.key);
							}
							else if(msg.type.equals("recover")){
								msg.h=new ConcurrentHashMap<String, String>();
								for(Map.Entry<String, String> entry : h.entrySet()) {

									if(check(entry.getKey(),msg.arr.get(0)) || check(entry.getKey(),msg.arr.get(1)) || check(entry.getKey(),msg.arr.get(2))){
										msg.h.put(entry.getKey(),entry.getValue());
									}
								}

								msg.type="recover_return";
								client(msg,msg.origin*2);
							}
							else if(msg.type.equals("recover_return")){
									h.putAll(msg.h);
									rec++;
							}
							else if(msg.type.equals("test")){
								if(h.isEmpty()) msg.myPort=1;
								else msg.myPort=2;
								msg.type="test_return";
								client(msg,msg.origin*2);
							}
							else if(msg.type.equals("test_return")){
								if(msg.myPort==2) second=true;
								test=true;
							}
//							else if(msg.type.equals("check_for")){
//								if(recover==false){
//									try {
//										Thread.sleep(1000);
//									}
//									catch(Exception e){
//										Log.d(TAG,e.toString());
//									}
//								}
//								msg.type = "check_for_return";
//								client(msg,msg.origin*2);
//								sem=false;
//							}
//							else if(msg.type.equals("check_for_return")){
//								Log.d(TAG, " Wo Rahi");
////								check_for++;
//								sem=true;
//							}


						}catch(Exception e){
							Log.e(TAG,e.toString()+msg.type);
						}
					}
				} catch (IOException e) {
					Log.e(TAG, "IOException1");
				} catch (Exception e) {
					Log.e(TAG, e.toString());
				}
			}
		}).start();
	}
	private boolean check(String key,int port){
		int i=arr.indexOf(port);
		String hash=null;
		String mhash=null;
		try{
		hash = genHash(key);
		mhash=genHash(String.valueOf(port));




		if(i==0) {
			if (hash.compareTo(mhash) <= 0 || ((hash).compareTo(genHash(String.valueOf(arr.get(4)))) > 0 && hash.compareTo(mhash) > 0)){
				return true;
			}
		}
		else if(hash.compareTo(mhash) <= 0 && hash.compareTo(genHash(String.valueOf(arr.get(i-1))))>0){
				return true;
		}

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, e.getMessage());
		}
		return false;

	}


	private Cursor Allquery(String key1,int origin) {

		MatrixCursor cursor = new MatrixCursor(new String[]{key, value});

		if (myPort == myPred || key1.equals("@") ) {
			for(Map.Entry<String, String> entry : h.entrySet()) {
				Log.d(TAG,"h@"+entry.getKey());
				cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
			}
			insert=true;squery=true;recover=true;
//			Message msg = new Message();
//			msg.type = "check_for_return";
//			msg.origin = myPort;
//			for(int j:arr){
//				if(j!=myPort)
//					client(msg,j*2);
//			}
			return cursor;
		} else {
			res.putAll(h);
			Message m = new Message();

			m.type = "send_client";
			m.origin = origin;
			m.key="AllQuery";
			for(int j:arr) {
				if(j!=myPort)
					client(m, j*2);

			}

//            try {
//                Thread.sleep(7000);
//            }
//            catch(Exception e){
//                Log.d(TAG,e.toString());
//            }
			while(aquery<arr.size()-1) {
//                try {
//                    Thread.sleep(500);
//                } catch (Exception e) {
//                    Log.d(TAG, e.toString());
//                }
			}
			aquery=0;

			for (Map.Entry<String, String> entry : res.entrySet()) {
				cursor.addRow(new String[]{entry.getKey(), entry.getValue()});
			}
		}
		insert=true;squery=true;recover=true;
//		Message msg = new Message();
//		msg.type = "check_for_return";
//		msg.origin = myPort;
//		for(int j:arr){
//			if(j!=myPort)
//				client(msg,j*2);
//		}
		return cursor;
	}


	private Cursor singlequery(String key1, int origin) {
//		while(squery==false);
//		squery=false;
		MatrixCursor cursor = new MatrixCursor(new String[]{key, value});
		Log.d(TAG,"here");
		try {
			String hash = genHash(key1);

			for(int i=0;i<5;i++){
				String mhash=genHash(String.valueOf(arr.get(i)));
				if(i==0) {
					if (hash.compareTo(mhash) <= 0 || ((hash).compareTo(genHash(String.valueOf(arr.get(4)))) > 0 && hash.compareTo(mhash) > 0)){
						Message msg=new Message();
						msg.key=key1;
//						msg.value=value;
						msg.type="query_get";
						msg.origin=myPort;
						msg.pred=arr.get(i+1);
						client(msg,arr.get(i+2)*2);
						while(squery==false){}
//						squery=false;
						break;
					}
				}
				else if(hash.compareTo(mhash) <= 0 && hash.compareTo(genHash(String.valueOf(arr.get(i-1))))>0){
					Message msg=new Message();
					msg.key=key1;
//					msg.value=value;
					msg.type="query_get";
					msg.origin=myPort;
					msg.pred=arr.get((i+1)%5);
					client(msg, arr.get((i+2 )%5) * 2);
					while(squery==false){}
//					squery=false;
					break;
				}
			}

		} catch (NoSuchAlgorithmException e) {
			Log.e(TAG, e.getMessage());
		}

//		if(h.containsKey(key1)){
//			Log.d(TAG, "normal"+key1);
//
//			cursor.addRow(new String[]{key1,h.get(key1)});
//			return cursor;
//		}
//		else{
//			Log.d(TAG,"else"+key1);
//			Message m=new Message();
//			m.key=key1;
//			m.type="send_client";
//			m.origin=origin;
//			for(int j:arr){
//				if(j!=myPort)
//					client(m,j*2);
//			}
//            try {
//                Thread.sleep(4000);
//            }
//            catch(Exception e){
//                Log.d(TAG,e.toString());
//            }
////			while(squery==false) {
////                try {
////                    Thread.sleep(500);
////                } catch (Exception e) {
////                    Log.d(TAG, e.toString());
////                }
////			}
//			squery=false;

//		}
		insert=true;squery=true;recover=true;
//		Message msg = new Message();
//		msg.type = "check_for_return";
//		msg.origin = myPort;
//		for(int j:arr){
//			if(j!=myPort)
//				client(msg,j*2);
//		}

		return qcursor;

	}
	private void delete(String key) {
		if (key.equals("@") ) {
			h.clear();
		}
		else  if (key.equals("*")) {
			h.clear();
			Message m=new Message();
			m.type="delete";
			m.key="notcheck";
			for(int j:arr) {
				if (j != myPort)
					client(m, j * 2);
			}


		} else {
			if (h.containsKey(key)) {
				h.remove(key);
			} else {
				Message m=new Message();
				m.type="delete";
				m.key=key;
				for(int j:arr) {
					if (j != myPort)
						client(m, j * 2);
				}

			}

		}
	}

}
