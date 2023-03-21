import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
/*
Elif Yildiz
211101046
*/

public class JobScheduler {
    

    class MinHeap<E extends Comparable<E>> {
        private ArrayList<E> heap;

        MinHeap() {
            heap = new ArrayList<>();

        }

        public boolean isEmpty() {
            if (heap.size() == 0)
                return true;

            return false;
        }

        public void insert(E item) {
            heap.add(item);
            int i = heap.size() - 1;
            int parent = parent(i);

            while (parent != i && heap.get(i).compareTo(heap.get(parent)) < 0) {
                swap(i, parent);
                i = parent;
                parent = parent(i);
            }
        }

        public E remove() {
            E root;
            if (size() == 0)
                return null;
            if (size() == 1) {
                root = heap.remove(0);
                return root;
            }

            root = heap.get(0);
            E lastItem = heap.remove(size() - 1);
            heap.set(0, lastItem);

            heapifyDown(0);

            return root;
        }

        public E getRoot() {
            return heap.get(0);
        }

        private void heapifyDown(int i) {
            int left = left(i);
            int right = right(i);
            int smallest = i;
            if (left < size() && heap.get(left).compareTo(heap.get(i)) < 0) {
                smallest = left;
            }
            if (right < size() && heap.get(right).compareTo(heap.get(smallest)) < 0) {
                smallest = right;
            }
            if (smallest != i) {
                swap(i, smallest);
                heapifyDown(smallest);
            }
        }

        public String print() {

            Job a[] = new Job[heap.size() + 1];
            for (int i = 0; i < a.length - 1; i++) {
                a[i] = (Job) heap.get(i);

            }
            if (a.length > 1)
                a[a.length - 1] = a[a.length - 2];

            StringBuilder sb = new StringBuilder();

            int max = 0;
            for (int i = 0; i < a.length; i++) {
                for (int j = 0; j < Math.pow(2, i) && j + Math.pow(2, i) < a.length; j++) {

                    if (j > max) {
                        max = j;
                    }
                }

            }
            if(heap.size()>1){
                for (int i = 0; i < a.length/2; i++) {
                    sb.append("   ");
                }
                sb.append(a[0].ID);
                sb.append("\n");
            for (int i = 1; i < a.length; i++) {
                for (int j = 0; j < Math.pow(2, i) && j + Math.pow(2, i) < a.length; j++) {

                    //sb.append("  ");
                    for (int k = 0; (k < max / ((int) Math.pow(2, i))); k++) {
                        sb.append("         ");
                    }
                    sb.append(a[j + (int) Math.pow(2, i) - 1].ID + "         ");

                }
                sb.append("\n");

            }
            }
            else{
                sb.append(a[0].ID);
            }
            return sb.toString();


        }

        public boolean validate() {
            for (int i = 1; i < heap.size(); i++) {
                if (heap.get(i).compareTo(heap.get(parent(i))) < 0) {
                    print();
                    return false;
                }
            }
            print();
            return true;
        }

        public int size() {
            return heap.size();
        }

        public void clear() {
            heap.clear();
        }

        private int parent(int i) {
            if (i == 0) { /* if i is already a root node */
                return 0;
            }
            return (i - 1) / 2;
        }

        private int left(int i) {
            return (2 * i + 1);
        }

        private int right(int i) {
            return (2 * i + 2);
        }

        private void swap(int i, int parent) {
            E tmp = heap.get(parent);
            heap.set(parent, heap.get(i));
            heap.set(i, tmp);
        }

    }


    public class Job implements Comparable<Job> {

        int ID;
        int arrivalTime;//geliş zamanı
        int timeToComplete;//bitmesi için çalıştırılması gereken süre
        int workedTime = 0;//ne kadar çalıştırıldığı
        int ResourceID;//çalıştıran kaynağın ıdsi
        int starTime;//çalıştırılmaya başlandığı zaman

        ArrayList<Integer> dependenciesList = new ArrayList<Integer>();//bağlı olduğu diğer joblar burada tutulur

        public Job(int iD, int arrivalTime, int timeToComplete) {
            ID = iD;
            this.arrivalTime = arrivalTime;
            this.timeToComplete = timeToComplete;


        }

        public int compareTo(Job j) {//minheap de kullanmak için gelme zamanlarına göre kıyas yapan metod
            if (arrivalTime > j.arrivalTime) {
                return 1;
            } else if (arrivalTime == j.arrivalTime) {
                return 0;
            } else {
                return -1;
            }
        }


    }

    public class Resource {
        int ID;
        boolean available;//kaynağın meşgul olup olmadığını tutar
        ArrayList<Job> assignedJobs;//kaynağın işlediği jobları tutar

        public Resource(int ID) {
            this.ID = ID;
            available = true;
            assignedJobs = new ArrayList<Job>();

        }

        public void add(Job j) {
            assignedJobs.add(j);  
        }



    }

    public MinHeap<Job> schedulerTree;
    public Integer timer = 0;
    public String filePath;
    public HashMap<Integer, ArrayList<Integer>> dependencyMap; // you can change Hashmap as Hashmap<Integer,Integer> or any other type


    public String dependencyPath;
    public Integer resourceNum;

    public FileReader file;
    public BufferedReader br;
    public boolean isThereALine = false;
    public FileReader f;
    public BufferedReader brr;

    public Resource[] resourceArray;//bütün kaynaklar burada tutulur

    public ArrayList<Job> done = new ArrayList<>();//çalışma süresi bitmiş job ları 
    //uygun bir veri yapısına kaydeder

    public ArrayList<Job> dependencyBlocked = new ArrayList<>();//dependency den dolayı çalıştırılamayan jobları depolar
    public ArrayList<Job> resourceBlocked = new ArrayList<>();//kaynak yetersizliğinden dolayı çalıştırılamayan jobları depolar
    public ArrayList<Job> working = new ArrayList<>();//üstünde çalışılan jobları depolar

    int remaing;

    public JobScheduler(String filePath) {//const

        this.filePath = filePath;
        //dosyayı okumak için gerekli işlemler
        try {

            file = new FileReader(filePath);
            br = new BufferedReader(file);

            f = new FileReader(filePath);
            brr = new BufferedReader(f);

        } catch (FileNotFoundException e) {

            e.printStackTrace();
        }
        schedulerTree = new MinHeap<>();

    }

    public void insertDependencies(String dependencyPath) {//hashmape dependencyler yerleştirilir

        this.dependencyPath = dependencyPath;
        dependencyMap = new HashMap<>();

        //file reading
        try {
            String line;
            FileReader file = new FileReader(dependencyPath);
            BufferedReader br = new BufferedReader(file);

            while ((line = br.readLine()) != null) {
                String[] words = line.split(" ");

                int zero = Integer.parseInt(words[0]);
                int one = Integer.parseInt(words[1]);

                if (dependencyMap.containsKey(zero) == false) {
                    ArrayList<Integer> a = new ArrayList<Integer>();
                    a.add(one);
                    dependencyMap.put(zero, a);
                } else {
                    dependencyMap.get(zero).add(one);
                }
            }

            /*  Check if hashmap has right inputs

            for(Map.Entry m : dependencyMap.entrySet()){    
                System.out.println(m.getKey()+" "+ m.getValue());    
               } 
            */

            br.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public boolean stillContinues() {//satırlar bitene kadar okumayı sağlar

        try {

            if ((brr.readLine()) != null) {
                isThereALine = true;
                //System.out.println("there is a line");
            } else {
                isThereALine = false;
                brr.close();
                // System.out.println("no line");
            }
        } catch (IOException e) {
           
            e.printStackTrace();
        }


        return isThereALine;
    }

    public void run() {

        work();//working listesindeki bütün jobların workedTime ı 1 arttırılır
        completed();//bitmiş job kontrolu yapılır


        boolean hasAvaibleResource = false;//boşta kaynak var mı kontrolü yapılır
        for (int i = 0; i < resourceArray.length; i++) {
            if (resourceArray[i].available == true)
                hasAvaibleResource = true;
        }


        while (!(schedulerTree.isEmpty())) {//bütün heap boşalana kadar heapdeki joblar uygun listelere dağıtılır

            Job j = schedulerTree.getRoot();

            Boolean dependenciesDone = false;
            //job un dependent olduğu jobların tamamlanmış olup olmadığı kontrol edilir

            int l = 0;
            for (int i = 0; i < j.dependenciesList.size(); i++) {
                for (int k = 0; k < done.size(); k++) {
                    int a = done.get(k).ID;
                    int b = j.dependenciesList.get(i);

                    if (a == b) {
                        l++;
                    }
                }
            }


            if (l == j.dependenciesList.size()) {
                dependenciesDone = true;
            }

            hasAvaibleResource = false;
            for (int i = 0; i < resourceArray.length; i++) {
                if (resourceArray[i].available == true)
                    hasAvaibleResource = true;
            }
            if (dependenciesDone) {

                if (hasAvaibleResource) {
                    schedulerTree.remove();//heapden silinir
                    working.add(j);//working e eklenir

                    for (int i = 0; i < resourceArray.length; i++) {
                        if (resourceArray[i].available) {
                            resourceArray[i].available = false;//jobu çalıştıracak kaynak meşgul yapılır
                            j.starTime = timer;
                            resourceArray[i].add(j);
                            j.ResourceID = resourceArray[i].ID;
                            i = resourceArray.length;
                        }
                    }
                    for (int i = 0; i < dependencyBlocked.size(); i++) {//eğer eklenen job dependencyBlocked listesindeyse oradan çıkarılır
                        if (dependencyBlocked.get(i).ID == j.ID) {
                            dependencyBlocked.remove(i);
                        }
                    }
                    for (int i = 0; i < resourceBlocked.size(); i++) {//eğer eklenen job resourceBlocked listesindeyse oradan çıkarılır
                        if (resourceBlocked.get(i).ID == j.ID) {
                            resourceBlocked.remove(i);
                        }
                    }

                } else {//kaynaklar müsait olmadığından resourceblocked a eklenir
                    schedulerTree.remove();
                    if (resourceBlocked.indexOf(j) == -1)//tekrar eklenmemesi için
                        resourceBlocked.add(j);

                }
            } else {//bütün dependencyleri tamamlanmadığından buraya eklenir

                schedulerTree.remove();
                if (dependencyBlocked.indexOf(j) == -1) {
                    dependencyBlocked.add(j);
                }
            }

            completed();
        }

        for (int i = 0; i < dependencyBlocked.size(); i++) {//heape tekrar dağıtılmak üzere geri eklenir
            schedulerTree.insert(dependencyBlocked.get(i));
        }
        for (int i = 0; i < resourceBlocked.size(); i++) {//heape tekrar dağıtılmak üzere geri eklenir
            schedulerTree.insert(resourceBlocked.get(i));
        }


    }

    public void work() {
        for (int i = 0; i < working.size(); i++) {
            working.get(i).workedTime = working.get(i).workedTime + 1;
        }
    }

    public void  completed(){
        for (int i = 0; i < working.size(); i++) {

            if (working.get(i).workedTime >= working.get(i).timeToComplete) {

                done.add(working.get(i));
                for (int l = 0; l < resourceArray.length; l++) {
                    for (int y = 0; y < resourceArray[i].assignedJobs.size(); y++) {

                        if (resourceArray[i].assignedJobs.get(y) != null) {

                            if (resourceArray[i].assignedJobs.get(y).ID == working.get(i).ID) {
                                resourceArray[l].available = true;
                            }
                        }

                    }
                }
                working.remove(i);
                i--;

            }

        }
    }

    public void setResourcesCount(Integer count) {//verilen sayı kadar kaynak oluşturur
        resourceNum = count;
        resourceArray = new Resource[count];

        for (int i = 0; i < count; i++) {
            Resource r = new Resource(i + 1);
            resourceArray[i] = r;
        }
    }

    public void insertJob() {//zamanı arttırır bir sonraki satırdaki jobu heape ekler

        timer++;
        try {
            String line = br.readLine();
            String[] words = line.split(" ");
            //System.out.println(words[0] + " " + words[1]);
            if (words[0].equals("no")) {

            } else {

                int zero = Integer.parseInt(words[0]);
                int one = Integer.parseInt(words[1]);
                Job j = new Job(zero, timer, one);

                for (Map.Entry m : dependencyMap.entrySet()) {
                    if (m.getKey() == (Integer) j.ID) {
                        j.dependenciesList = (ArrayList<Integer>) m.getValue();
                    }
                }
                //min heap e ekle;
                schedulerTree.insert(j);
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

    public void completedJobs() {
        System.out.print("completed jobs ");

        if (done.size() > 0)
            System.out.print(done.get(0).ID);
        for (int i = 1; i < done.size(); i++) {
            System.out.print(", " + done.get(i).ID);
        }
        System.out.println();

    }

    public void dependencyBlockedJobs() {
        System.out.print("dependency blocked jobs ");

        if (dependencyBlocked.size() > 0)
            //System.out.print(dependencyBlocked.get(0).ID);
            for (int i = 0; i < dependencyBlocked.size(); i++) {
                System.out.print("(" + dependencyBlocked.get(i).ID + "," + dependencyBlocked.get(i).dependenciesList.get(dependencyBlocked.get(i).dependenciesList.size() - 1) + ")");
            }
        System.out.println();

    }

    public void resourceBlockedJobs() {

        System.out.print("resource blocked jobs ");

        if (resourceBlocked.size() > 0)
            System.out.print(resourceBlocked.get(0).ID);
        for (int i = 1; i < resourceBlocked.size(); i++) {
            System.out.print(", " + resourceBlocked.get(i).ID);
        }
        System.out.println();


    }

    public void workingJobs() {
        System.out.print("working jobs ");

        if (working.size() > 0)
            //System.out.print(working.get(0).ID);
            for (int i = 0; i < working.size(); i++) {
                System.out.print("(" + working.get(i).ID + "," + working.get(i).ResourceID + ") ");
            }
        System.out.println();


    }

    public void runAllRemaining() {
        remaing = working.size() + resourceBlocked.size() + dependencyBlocked.size();

        while (remaing != 0){//3 listede de herhangi bir eleman kalmayana kadar devam eder
            timer++;
            run();
            /* 
            dependencyBlockedJobs();
            resourceBlockedJobs();
            workingJobs();
            System.out.println("-------------" + timer + "-------------");
            */
            remaing = working.size() + resourceBlocked.size() + dependencyBlocked.size();

        }

    }

    public void allTimeLine() {

       System.out.println("");
       for (int i = 0; i < resourceArray.length; i++) {
        System.out.print("       "+"R" + resourceArray[i].ID);
       }
       System.out.println("");
       for (int i = 1; i < timer; i++) {
            System.out.print(i + ":");
             
            for (int j = 0; j < resourceArray.length; j++) {
                for (int j2 = 0; j2 < resourceArray[j].assignedJobs.size(); j2++) {
                    if(resourceArray[j].assignedJobs.get(j2).starTime == i  && resourceArray[j].assignedJobs.get(j2).workedTime != 0){
                        resourceArray[j].assignedJobs.get(j2).starTime++;
                        resourceArray[j].assignedJobs.get(j2).workedTime--;
                        System.out.print("      " + resourceArray[j].assignedJobs.get(j2).ID );
                        
                    }
                }
            }
            
            System.out.println("");
       }


    }

    public String toString() {
        if (schedulerTree.isEmpty())
            return "";
        else
            return schedulerTree.print();
    }
}