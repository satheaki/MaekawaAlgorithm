package com.aos.algorithm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.Scanner;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

/**
 * @author Akshay Darshan Arnab Aniruddha
 *
 */
public class Process implements NetworkInterface {

	// Token based

	private PriorityQueue<AbstractProcess> mNodeRequestQueue = new PriorityQueue<AbstractProcess>();
	private boolean isUsing = false;
	private boolean mHasToken = false;
	private boolean mAsked = false;
	private boolean mHasRequested = false;

	// Gson
	private Gson mGson;

	private AbstractProcess mSelf;

	public int mProcessId;
	public int[] mQuorumId;
	private NetworkWrapper mNetworkWrapper;
	private PriorityQueue<AbstractProcess> mProcessRequestQueue = new PriorityQueue<AbstractProcess>();

	// Indicates if current process is locked for other process
	private volatile boolean isLocked = false;

	// Indicates if current process has ever received a failed message
	private volatile boolean hasReceivedFailedMessage = false;

	// Indicates if current process has ever received an inquire message
	private volatile boolean hasReceivedInquireMessage = false;

	/**
	 * This process will be used in case of inquire message. When an inquire
	 * message is received, this process will indicate which process has sent an
	 * inquire message. In future when a failed message is received, a
	 * relinquish message will be sent to this process
	 */
	private AbstractProcess mRelinquishDestination = null;

	private static int  mIncomingLockCounter;

	private static int  mNumberOfNodes = 0;

	private static int mTimeInCriticalSection;

	// HashMaps
	private HashMap<Integer, String> mProcessIpAddresses;
	private HashMap<Integer, Integer> mProcessPorts;

	public Process(int pid, int[] qid, int nodeid) throws NumberFormatException,
			IOException {
		addShutdownHook(this);
		// Init server
		this.mNetworkWrapper = NetworkWrapper.getInstance(this);
		// Save info
		this.mProcessId = pid;
		
		for(int i=0;i<qid.length;i++){
			this.mQuorumId[i]=qid[i];
		}
		// Currently process is not locked for any other process
		isLocked = false;
		this.mProcessIpAddresses = new HashMap<Integer, String>();
		this.mProcessPorts = new HashMap<Integer, Integer>();
		this.initProcesses();
		this.mGson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
				.create();
		this.mSelf = new AbstractProcess(this.mProcessId, 0);
	}

	/**
	 * Basic init operations
	 * 
	 * @throws IOException
	 * @throws NumberFormatException
	 */
	private void initProcesses() throws NumberFormatException, IOException {
		// // Init ip addresses
		// this.mProcessIpAddresses.put(1, "192.168.0.9");
		// this.mProcessIpAddresses.put(2, "192.168.0.2");
		// this.mProcessIpAddresses.put(3, "192.168.0.4");
		//
		// // Init ports
		// this.mProcessPorts.put(1, 5051);
		// this.mProcessPorts.put(2, 5051);
		// this.mProcessPorts.put(3, 5051);

		File configFile = new File("Config.txt");
		BufferedReader buffFileReader = null;
		try {
			buffFileReader = new BufferedReader(new FileReader(configFile));
			String line = null;
			while ((line = buffFileReader.readLine()) != null) {
				String[] processInfo = line.split("	");

				/* Placing ProcessId,port and IpAddress in hashMap */
				this.mProcessIpAddresses.put(Integer.parseInt(processInfo[0]),
						processInfo[1]);
				this.mProcessPorts.put(Integer.parseInt(processInfo[0]),
						Integer.parseInt(processInfo[2]));

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	// Function that intiates execution of critical section
	private void executeCriticalSection() {
		if (this.mProcessId == 0)
			this.requestToken();
		this.mHasRequested = true;
		this.sendRequestMessage();
	}

	private synchronized void sendRequestMessage() {
		String ip;
		int port;
		int clockValue = LamportClock.getClockValue();
		this.mSelf.setTimestamp(clockValue);
		JSONObject object;
		this.mProcessRequestQueue.add(new AbstractProcess(this.mProcessId,
				clockValue));
		for (int i = 1; i <= this.mProcessPorts.size(); i++) {
			if (i == this.mProcessId)
				continue;
			ip = this.mProcessIpAddresses.get(i);
			port = this.mProcessPorts.get(i);
			object = new JSONObject();
			try {
				object.put(Constants.MESSAGE_TYPE, Constants.TYPE_REQUEST);
				object.put(Constants.MESSAGE_INFO,
						new JSONObject(this.mGson.toJson(this.mSelf)));
				// TODO set appropriate size()
				this.mIncomingLockCounter = this.mProcessIpAddresses.size() - 1;
				this.mNetworkWrapper.sendMessage(object.toString(), ip, port);
			} catch (JSONException e) {
				e.printStackTrace();
				return;
			}
		}
	}

	public static void main(String args[]) throws Exception {
		int[] quorumId=readInput();
		Process process = new Process(2, quorumId, 0);
		Thread.sleep(5000);
		process.executeCriticalSection();
		Scanner scanner = new Scanner(System.in);
		String input = scanner.nextLine();
		if (input.equalsIgnoreCase("end"))
			System.exit(0);
		scanner.close();
	}

	/**
	 * Function to read the input file containing the information on Quorums
	 */
	private static int[] readInput() {
		int i=0;
		int[] quorumId = null;
		File inputFile = new File("Quorum.txt");
		BufferedReader buffFileReader = null;
		try {
			buffFileReader = new BufferedReader(new FileReader(inputFile));
			mNumberOfNodes = Integer.parseInt(buffFileReader.readLine().split(
					"=")[1]);
			mTimeInCriticalSection = Integer.parseInt(buffFileReader.readLine()
					.split("=")[1]);
			String line = null;
			
			while ((line = buffFileReader.readLine()) != null) {

				/*
				 * quorumInfo[0] contains Q0 and quorumInfo[1] contains members
				 * of quorum
				 */
				String[] quorumInfo = line.split("=");
				quorumId[i] = Integer.parseInt(quorumInfo[0].substring(1));
				i++;
			}
		} catch (Exception e) {

		}
		
		return quorumId;
	}

	public static void addShutdownHook(Process p) {
		Runtime runtime = Runtime.getRuntime();
		runtime.addShutdownHook(new Thread() {
			@Override
			public void run() {
				NetworkWrapper.getInstance(p).closeServer();
			}
		});
	}

	@Override
	public void messageReceived(String message) {
		JSONObject object = null;
		try {
			object = new JSONObject(message);
			switch (object.getString(Constants.MESSAGE_TYPE)) {

			// Incoming locked message
			case Constants.TYPE_LOCK:
				this.checkForCriticalExecution(object);
				break;

			// Incoming request message by other process
			case Constants.TYPE_REQUEST:
				this.processIncomingRequest(object);
				break;

			// Incoming failed message
			case Constants.TYPE_FAILED:
				this.processFailedMessage(object);
				break;

			// Incoming inquire message
			case Constants.TYPE_INQUIRE:
				this.processInquireMessage(object);
				break;

			// Incoming relinquish message
			case Constants.TYPE_RELINQUISH:
				this.processRelinquishMessage(object);
				break;

			// Incoming release message
			case Constants.TYPE_RELEASE:
				this.processReleaseMessage(object);
				break;

			// Incoming token
			case Constants.TYPE_TOKEN:
				this.receivedToken();
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
			System.out.println("Gandla");
		}
	}

	private void processReleaseMessage(JSONObject object) {
		int toRemovePid;
		try {
			toRemovePid = object.getJSONObject(Constants.MESSAGE_INFO).getInt(
					Constants.PROCESS_ID);
			AbstractProcess toRemove = new AbstractProcess(toRemovePid, -1);
			this.mProcessRequestQueue.remove(toRemove);
			System.out.println("Lastest process: "
					+ this.mProcessRequestQueue.peek().getPID());
			this.isLocked = false;
			this.processNext();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processNext() {
		if (!this.mProcessRequestQueue.isEmpty()) {
			if (!this.mProcessRequestQueue.peek().equals(this.mSelf))
				this.sendLockedMessage(this.mProcessRequestQueue.peek());
		}
	}

	private void resetFlags() {
		this.isLocked = false;
		this.hasReceivedFailedMessage = false;
		this.hasReceivedInquireMessage = false;
		this.mRelinquishDestination = null;
	}

	private void processRelinquishMessage(JSONObject object) {
		JSONObject lockMessage = new JSONObject();
		try {
			lockMessage.put(Constants.MESSAGE_TYPE, Constants.TYPE_LOCK);
			lockMessage.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			int toSendLockPid = this.mProcessRequestQueue.peek().getPID();
			this.mNetworkWrapper.sendMessage(lockMessage.toString(),
					this.mProcessIpAddresses.get(toSendLockPid),
					this.mProcessPorts.get(toSendLockPid));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processInquireMessage(JSONObject object) {
		int pid;
		try {
			pid = object.getJSONObject(Constants.MESSAGE_INFO).getInt(
					Constants.PROCESS_ID);
			AbstractProcess otherProcess = new AbstractProcess(pid, -1);
			this.mRelinquishDestination = otherProcess;
			if (this.hasReceivedFailedMessage) {
				this.sendRelinquishMessage(otherProcess);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processFailedMessage(JSONObject object) {
		try {
			this.hasReceivedFailedMessage = true;
			if (this.hasReceivedInquireMessage) {
				// There was a previous inquire message received, now send a
				// relinquish message
				this.sendRelinquishMessage(this.mRelinquishDestination);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void sendRelinquishMessage(AbstractProcess other) {
		System.out.println("Sending relinquish message: " + other.getPID());
		JSONObject object = new JSONObject();
		try {
			this.mIncomingLockCounter++;
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_RELINQUISH);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(other.getPID()),
					this.mProcessPorts.get(other.getPID()));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void processIncomingRequest(JSONObject object) {
		AbstractProcess topProcess = this.mProcessRequestQueue.peek();
		AbstractProcess incomingProcess;
		try {
			incomingProcess = new AbstractProcess(object.getJSONObject(
					Constants.MESSAGE_INFO).getInt("pid"), object
					.getJSONObject(Constants.MESSAGE_INFO).getInt("timestamp"));
			this.mProcessRequestQueue.add(incomingProcess);
			if (topProcess != null
					&& !topProcess.equals(this.mProcessRequestQueue.peek())
					&& this.isLocked) {
				// Conditions to check before sending inquire message
				// 1. isLocked = true i.e current process is locked for other
				// process
				// 2. queue after adding process to queue, compare if most
				// recent process has changed

				this.sendInquireMessage(topProcess);
			} else if (this.isLocked) {
				this.sendFailedMessage(incomingProcess);
			} else {
				this.sendLockedMessage(incomingProcess);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendLockedMessage(AbstractProcess incomingProcess) {
		System.out.println("Sending locked message: "
				+ incomingProcess.getPID());
		JSONObject object = new JSONObject();
		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_LOCK);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(incomingProcess.getPID()),
					this.mProcessPorts.get(incomingProcess.getPID()));
			this.isLocked = true;
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendFailedMessage(AbstractProcess incomingProcess) {
		System.out.println("Sending failed message: "
				+ incomingProcess.getPID());
		JSONObject object = new JSONObject();
		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_FAILED);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(incomingProcess.getPID()),
					this.mProcessPorts.get(incomingProcess.getPID()));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void sendInquireMessage(AbstractProcess incomingProcess) {
		System.out.println("Sending inquire: " + incomingProcess.getPID());
		JSONObject object = new JSONObject();
		try {
			object.put(Constants.MESSAGE_TYPE, Constants.TYPE_INQUIRE);
			object.put(Constants.MESSAGE_INFO,
					new JSONObject(this.mGson.toJson(this.mSelf)));
			this.mNetworkWrapper.sendMessage(object.toString(),
					this.mProcessIpAddresses.get(incomingProcess.getPID()),
					this.mProcessPorts.get(incomingProcess.getPID()));
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	private void checkForCriticalExecution(JSONObject object) {
		this.mIncomingLockCounter--;

		if (this.mIncomingLockCounter == 0) {
			// Received Lock from all processes of quorums
			try {
				this.criticalSection();
				this.sendReleaseMessage();
				// Token steps
				this.mHasRequested = false;
				this.isUsing = false;
				this.mAsked = false;
				if (!this.mNodeRequestQueue.isEmpty()) {
					this.requestToken();
					this.mAsked = true;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void sendReleaseMessage() {
		JSONObject object;
		System.out.println("Sending release message");
		for (int i = 1; i <= this.mProcessIpAddresses.size(); i++) {
			if (i == this.mSelf.getPID())
				continue;
			object = new JSONObject();
			try {
				object.put(Constants.MESSAGE_TYPE, Constants.TYPE_RELEASE);
				// TODO dummy value
				object.put(Constants.MESSAGE_INFO,
						new JSONObject(this.mGson.toJson(this.mSelf)));
				this.mNetworkWrapper.sendMessage(object.toString(),
						this.mProcessIpAddresses.get(i),
						this.mProcessPorts.get(i));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		this.resetFlags();
		if (this.mProcessRequestQueue.remove(this.mSelf))
			System.out.println("Removed from queue: " + this.mSelf.getPID());
		else
			System.out.println("There was an error removing process: "
					+ this.mSelf.getPID() + " from process queue");
		this.processNext();
	}

	private void criticalSection() throws InterruptedException {
		SimpleDateFormat format = new SimpleDateFormat("hh:mm:ss");
		System.out.println("Process: " + this.mProcessId
				+ " entering critical section: "
				+ format.format(new Date(System.currentTimeMillis())));
		Thread.sleep(10000);
		System.out.println("Process: " + this.mProcessId
				+ " exiting critical section: "
				+ format.format(new Date(System.currentTimeMillis()))
				+ "\n\n\n");
	}

	// Token based methods
	private synchronized void requestToken() {
		if (this.isUsing)
			return;
		if (this.mHasToken)
			this.executeCriticalSection();
		if (this.mAsked)
			return;
		this.mNodeRequestQueue.add(new AbstractProcess(this.mProcessId, 0));
		this.mNetworkWrapper.sendMessage("token",
				this.mProcessIpAddresses.get(this.mProcessId),
				this.mProcessPorts.get(this.mProcessId));
	}

	// Received Token
	private synchronized void receivedToken() {
		AbstractProcess current = this.mNodeRequestQueue.remove();
		this.mAsked = false;
		if (this.mHasRequested) {
			this.executeCriticalSection();
		} else {
			// Send to holder
			this.mNetworkWrapper.sendMessage(Constants.TYPE_TOKEN,
					this.mProcessIpAddresses.get(this.mProcessId),
					this.mProcessPorts.get(current.getPID()));
			if (!this.mNodeRequestQueue.isEmpty()) {
				this.requestToken();
				this.mAsked = true;
			}
		}
	}
}

class LamportClock {
	private static int clock = 0;

	private static void tick() {
		clock++;
	}

	public static int getClockValue() {
		tick();
		return clock;
	}
}

class AbstractProcess implements Comparable<AbstractProcess> {

	@Expose
	private int pid, timestamp;

	public AbstractProcess(int pid, int timestamp, String ip, int port) {
		this.pid = pid;
		this.timestamp = timestamp;
	}

	public void setTimestamp(int timestamp) {
		this.timestamp = timestamp;
	}

	public int getPID() {
		return this.pid;
	}

	public AbstractProcess(int pid, int timestamp) {
		this.pid = pid;
		this.timestamp = timestamp;
	}

	@Override
	public int compareTo(AbstractProcess o) {
		if (this.timestamp < o.timestamp) {
			return -1;
		} else if (this.timestamp > o.timestamp) {
			return 1;
		} else {
			if (this.pid < o.pid)
				return -1;
			else
				return 1;
		}
	}

	@Override
	public boolean equals(Object obj) {
		AbstractProcess other = (AbstractProcess) obj;
		return other.getPID() == this.pid;
	}
}