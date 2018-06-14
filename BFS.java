
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import javax.swing.plaf.synth.SynthSpinnerUI;



public class BFS {

	static int adjMatrix[][];
	static HashMap<Node,HashSet<Node>> adjList=new HashMap<Node,HashSet<Node>>();
	static Map<Node,PriorityQueue<Message>> messageQueue = new HashMap<>();
	static HashMap<Integer,Node> NodeMap=new HashMap<>();
	static boolean start=false;
	static int round=0;
	static boolean complete=false;
	static int source;
	
	
	public BFS(int num){
		adjMatrix=new int[num][num];
	}
	
	public static class Message{
		
		int distanceFromStart;
		int pid;
		int random;
		String type;
		public Message(int sender,int distanceFromStart,String type,int random){
			this.pid=sender;
			this.distanceFromStart=distanceFromStart;
			this.type=type;
			this.random=random;
		}
		
	}
	
	public static class Node implements Callable<Boolean>{
		
		Node parent;
		boolean ackSent;
		
		int pid,randomRound;
		int distanceFromStart;
		HashSet<Node> neighbors = new HashSet<Node>();
		HashSet<Node> nodeChildren=new HashSet<Node>();
		HashMap<Node,ArrayList<Message>> messageList=new HashMap<Node,ArrayList<Message>>();
		HashSet<Node> response=new HashSet<Node>();
		HashSet<Node> neighborMessage=new HashSet<>();
		int max=5,min=1;
		public Node(int pid){
			this.pid=pid;
			parent=null;
			ackSent=false;
		}
		@Override
		public Boolean call() throws Exception {
			if(start==true){
				sendMessage();
			}
			else{
				receiveMessage();
			}
			return true;
		
		}
		public int generateRandom(){
			
			int min=round+1;
			int max=round+18;
			Random rand=new Random();
			int num=rand.nextInt((max-min)+1)+min;
			System.out.println("random number  = "+num+" picked by "+"Pid = "+pid);
			return num;
		}
		/*public int repeatRandom(){
			
			if(round>1){
				if(round<max){
					min=max;
					max=min+5;
				}else{
					min=round;
					max=round+5;
				}
			}
			Random rand=new Random();
			int num=rand.nextInt((max - min) + 1) + min;
			
			return num;
		}*/
		public void receiveMessage() throws InterruptedException{
		
			boolean loopExit=false;
			while(!loopExit){	
				Message message=messageQueue.get(this).peek();
			
				if(message!=null){
					
					
					
				if(message.random==round){
					
					
					message=messageQueue.get(this).poll();
				
					if(message.type=="N"){
		
						if(distanceFromStart>message.distanceFromStart+adjMatrix[pid][message.pid]){
						
							distanceFromStart=message.distanceFromStart+adjMatrix[pid][message.pid];
							Node parentEx=this.parent;
							if(parentEx!=null){
								response.add(this.parent);
								randomRound=this.generateRandom();
								Message msg=new Message(pid,0,"R",randomRound);
								addNewMessage(parentEx,msg);
							}
						//System.out.println(this.pid+" "+message.pid);
							this.parent=NodeMap.get(message.pid); 
						//System.out.println("Current pid"+this.pid+" parent "+this.parent.pid);
							for(Node node:neighbors){
							
								System.out.println("From "+message.pid+" "+this.pid+" Prepping Normal message to "+node.pid);
								Message msg;	
								
								randomRound=this.generateRandom();
								msg=new Message(pid,distanceFromStart,"N",randomRound);
										
								addNewMessage(node,msg);
							
							
							}
						}else{
						
						System.out.println(this.pid+" Preparing to reject  "+message.pid);
						//rejects.add(NodeMap.get(message.pid));
						randomRound=this.generateRandom();
						Message msg=new Message(pid,0,"R",randomRound);
						addNewMessage(NodeMap.get(message.pid),msg);
						
						}
					}
				
					if(message.type=="R"){
						System.out.println("Rejected Candidate "+this.pid+" by "+message.pid);
						if(!response.contains(NodeMap.get(message.pid)))
							response.add(NodeMap.get(message.pid));
					
					}
					if(message.type=="A"){
						System.out.println("Accepted Candidate "+pid+" by "+message.pid);
						response.add(NodeMap.get(message.pid));
						nodeChildren.add(NodeMap.get(message.pid));
					
					}
				}else{
					loopExit=true;
				}
				}else{
					loopExit=true;
				}
				
			}

			if(response.size()==neighbors.size()&&!ackSent){

				if(this.pid==source){
					complete=true;
					
				}else{
					//System.out.println(this.pid+" Prepping acknowledgement to "+this.parent.pid);
					System.out.println("All messages received by "+pid);
					randomRound=this.generateRandom();
					
				Message msg=new Message(this.pid,0,"A",randomRound);
				addNewMessage(this.parent,msg);
				
				}
				ackSent=true;
				
			}
	
		
		}
		public void sendMessage(){
			
			for (Node node:messageList.keySet()){
				
				if(!messageList.get(node).isEmpty()){
					System.out.println("Message sent from  " + pid + " to " + node.pid);
					for(Message message:messageList.get(node)){
						messageQueue.get(node).offer(message);
					}
                //messageQueue.get(node).addAll(messageQueue.get(node));
				}
			
            }
			messageList.clear(); // Clear messagelist 
			
			
		}
		public void initializeNodes(int source){
		
			
				if(pid==source){
					distanceFromStart=0;
					
					
					for (Node neighbor : neighbors){
						int random=this.generateRandom();
						Message message=new Message(pid,distanceFromStart,"N",random);
						addNewMessage(neighbor, message);
	                }
				}
				else
					distanceFromStart=Integer.MAX_VALUE;
			}
		
		private void addNewMessage(Node neighbor, Message message) {
		
			ArrayList<Message> messages = messageList.get(neighbor);//, message);
            if (messages == null){
                messages = new ArrayList<Message>();
                messageList.put(neighbor, messages);
               
            }
            
            messages.add(message);
		}
			
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		
		Scanner sc=new Scanner(System.in);
		HashSet<Node> nodes=new HashSet<>();
		Queue<Integer> queue=new LinkedList<>();
		System.out.println("Enter the input file");
		String fileName=sc.nextLine();
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		String line=br.readLine();
		int num=Integer.parseInt(line);
		BFS b=new BFS(num);
		line=br.readLine();
		String split[]=line.split("\\s+");
		int counter=0;
		for(int i=0;i<split.length;i++){
			int pid=Integer.parseInt(split[counter++]);
			Node node=new Node(pid);
			nodes.add(node);
			NodeMap.put(pid, node);
			queue.add(pid);

		}
		
		line=br.readLine();
		source=Integer.parseInt(line);
		
		while((line=br.readLine())!=null){
			
			String splitLine[]=line.split("\\s+");
			int neighbour=0;  //neighbour for current node
			int pid=queue.poll();
				for(String s:splitLine){
					
					if(s.equals("-1")){
						neighbour++;
						continue;
					}
					for(Node node:nodes){
						
						if(node.pid==neighbour){
							NodeMap.get(pid).neighbors.add(node);
						}
					}
					
					adjMatrix[pid][neighbour]=Integer.parseInt(s);
					adjMatrix[neighbour][pid]=Integer.parseInt(s);
					neighbour++;
				}
				messageQueue.put(NodeMap.get(pid), new PriorityQueue<Message>(randomComparator));
				
			//	adjList.put(NodeMap.get(pid), node.neighbors);
			
			
		}
		
		/*for(Node n:nodes){
			System.out.println("");
			System.out.print(n.pid+"->");
			for(Node neighbour:n.neighbors){
				System.out.print(neighbour.pid+" ");
			}
		}*/
		
		for(Node node:nodes){
			node.initializeNodes(source);
		}
	
		b.start(nodes,num);
	}
	
	public static Comparator<Message> randomComparator=new Comparator<Message>(){

		@Override
		public int compare(Message arg0, Message arg1) {
			// TODO Auto-generated method stub
			if(arg0.random>arg1.random)
				return 1;
			if(arg0.random<arg1.random)
				return -1;
			
			return 0;
		}
		
	};
	public void start(HashSet<Node> nodes,int num) throws InterruptedException, ExecutionException{

		while(!complete){
			start=true;
			ExecutorService execStart=Executors.newFixedThreadPool(1);
			List<Future<Boolean>> futuresStart=execStart.invokeAll((Collection<? extends Callable<Boolean>>) nodes);
			execStart.shutdown();
		
			for(Future<Boolean> future:futuresStart){
				future.get();
			}
			start=false;
			ExecutorService execEnd=Executors.newFixedThreadPool(1);
			List<Future<Boolean>> futuresEnd=execEnd.invokeAll((Collection<? extends Callable<Boolean>>) nodes);
			execEnd.shutdown();
		
			for(Future<Boolean> future:futuresEnd){
				future.get();
			
			}
			start=true;	
			
			
			System.out.println("Round "+ round+" complete -> All messages delivered");
			System.out.println("-----------------------------------------------------");
			round++;
			if(complete){
				break;
			}
		}// end of while
		System.out.println("-------------------"); 
		System.out.println("OUTPUT");
		System.out.println("-------------------");
		for(Node node:nodes){
			if(!node.nodeChildren.isEmpty()){
				for(Node n:node.nodeChildren){
					System.out.println(node.pid+"-> "+n.pid+" (Hops = "+n.distanceFromStart+")");
				}
			}
		}
	}

}
