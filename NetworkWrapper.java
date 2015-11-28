package com.aos.algorithm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * @author Akshay Darshan Arnab Aniruddha
 *
 */

public class NetworkWrapper {

	private ServerSocket mServerSocket;
	private NetworkInterface mInterface;
	private static NetworkWrapper sNetworkWrapper;
	private volatile boolean isRunning = true;

	private NetworkWrapper(NetworkInterface nInterface) {
		this.mInterface = nInterface;
		try {
			mServerSocket = new ServerSocket(5051);
			new Thread() {
				@Override
				public void run() {
					while (isRunning) {
						Socket sock;
						try {
							sock = mServerSocket.accept();
							Thread t = new Thread(new ClientHandler(sock));
							t.start();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public synchronized static NetworkWrapper getInstance(
			NetworkInterface nInterface) {
		if (NetworkWrapper.sNetworkWrapper == null) {
			NetworkWrapper.sNetworkWrapper = new NetworkWrapper(nInterface);
		}
		return sNetworkWrapper;
	}

	public void sendMessage(String message, String address, int port) {
		new Thread() {
			private PrintWriter mWriter;

			@Override
			public void run() {
				try {
					Socket sock = new Socket(address, port);
					mWriter = new PrintWriter(sock.getOutputStream());
					mWriter.println(message);
					mWriter.flush();
					sock.close();
				} catch (UnknownHostException e) {

					e.printStackTrace();
				} catch (IOException e) {

					e.printStackTrace();
				}
			}
		}.start();
	}

	private class ClientHandler implements Runnable {
		private Socket mSock;
		private BufferedReader mReader;
		private String message;

		public ClientHandler(Socket newSock) {

			try {
				mSock = newSock;
				mReader = new BufferedReader(new InputStreamReader(
						mSock.getInputStream()));

			} catch (IOException e) {

				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			String finalMessage = "";
			try {
				while ((message = mReader.readLine()) != null) {
//					System.out.println("Client: " + message);
					finalMessage += message;
				}
				System.out.println(finalMessage);
			} catch (IOException e) {

			}

			NetworkWrapper.this.mInterface.messageReceived(finalMessage);

		}

	}

	public void closeServer() {
		if (this.isRunning && this.mServerSocket != null) {
			System.out.println("closing server");
			this.isRunning = false;
			try {
				this.mServerSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
