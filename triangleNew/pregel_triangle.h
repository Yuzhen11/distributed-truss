#include "basic/pregel-dev.h"
#include <sstream>
#include <set>
#include <map>
using namespace std;
 
struct triValue {
    int K;
    vector<VertexID> edges;
    vector<int> triangleNum;
    vector<int> done; //whether the edge is done
    vector<vector<VertexID> > tri; //the edge form triangle with whom
    int check; //whether the vertex is sending messages
    vector<set<VertexID> >mySet; //the edge is updated by which vertex 
    vector<VertexID> ans; //k-truss
    
    
    map<int,int> mm;
};


ibinstream& operator<<(ibinstream& m, const triValue& v)
{
    m << v.K;
    m << v.edges;
    m << v.triangleNum;
    m << v.done;
    m << v.tri;
    m << v.check;
    m << v.mySet;
    m << v.ans;
    m << v.mm;
    return m;
}

obinstream& operator>>(obinstream& m,triValue& v)
{
    m >> v.K;
    m >> v.edges;
    m >> v.triangleNum;
    m >> v.done;
    m >> v.tri;
    m >> v.check;
    m >> v.mySet;
    m >> v.ans;
    m >> v.mm;
    return m;
}
//---------------------------
struct edgeValue
{
    VertexID v;
    vector<VertexID> edges;
    
    edgeValue(){}
    edgeValue(VertexID newV, vector<VertexID> newEdges):v(newV), edges(newEdges){}
};
ibinstream& operator<<(ibinstream& m, const edgeValue& e)
{
    m << e.v;
    m << e.edges;
    return m;
}

obinstream& operator>>(obinstream& m,edgeValue& e)
{
    m >> e.v;
    m >> e.edges;
    return m;
}
//------------------------------

//====================================


VertexID mini(VertexID v1, VertexID v2)
{
	if (v1 < v2) return v1;
	else return v2;
}
//vertex
class triVertex : public Vertex<VertexID, triValue, edgeValue> {
public:
    virtual void compute(MessageContainer& messages)
    {
    	//if (step_num() > 12) {vote_to_halt();return;}
    	
        if (step_num() == 1)
	{	     
	     vector<VertexID> send;
	     for (int i = value().edges.size()-1; i >= 0; --i)
	     {
	     	if (value().edges[i] < id) 
	     	{
	     		send_message(value().edges[i], edgeValue(id, send));
	     	}
	     	if (value().edges[i] > id) send.push_back(value().edges[i]);
	     }
	}
	else if (step_num() == 2)
	{
	     //find triangle
	     for (int i = 0; i < messages.size(); ++ i)
	     {
	     	  VertexID& vid = messages[i].v;
	     	  vector<VertexID>& vedges = messages[i].edges;
		  //reverse
		  for (int k = 0; k < vedges.size()/2; ++ k) swap(vedges[k], vedges[vedges.size()-1-k]);
		  

	     	  int j = value().mm[vid];
	     	  
	     	  	if (value().edges[j] == vid)
	     	  	{
	     	  	     int p1 = 0;
	     	  	     int p2 = 0;
	     	  	     int ans = 0;
	     	  	     while(p1 < value().edges.size() && p2 < vedges.size())
	     	  	     {
	     	  	     	if (value().edges[p1] == vedges[p2]) 
	     	  	     	{
		     	  	     	value().tri[j].push_back(p1); //put tri;
		     	  	     	
		     	  	     	//find w
		     	  	     	//naive
		     	  	     	vector<VertexID> dump;
		     	  	     	dump.push_back(id);

		     	  	     	int k = value().mm[value().edges[p1]];
		     	  	     	
	     	  	     		if (value().edges[k] == value().edges[p1])
	     	  	     		{
	     	  	     			value().tri[k].push_back(value().mm[vid]);

	     	  	     			send_message(vid, edgeValue(value().edges[k], dump));

	     	  	     		
	     	  	     		}
		     	  	     	
		     	  	     	ans ++; p1++; p2++; 
	     	  	     	}
	     	  	     	else if (value().edges[p1] < vedges[p2]) {p1++;}
	     	  	     	else p2++;
	     	  	     }
	     	  	     value().triangleNum[j] = ans; //find sup

	     	  	     continue;
	     	  	}
	     	  //}
	     	  
	     }	
	}
	else if (step_num() == 3)
	{
		//find triangle
	     for (int i = 0; i < messages.size(); ++ i)
	     {
	     	  VertexID& vid = messages[i].v;
	     	  vector<VertexID>& vedges = messages[i].edges;
	     	  
	     	  /*
	     	  //binary search
	     	  int j;
	     	  int l = 0;int r = value().edges.size()-1; int m;
	     	  while (l <= r)
	     	  {
	     	  	m = l+(r-l)/2;
	     	  	if (value().edges[m] == vid) {j = m; break;}
	     	  	else if (value().edges[m] > vid) r = m-1;
	     	  	else l = m+1;
	     	  }
	     	  */
	     	  int j = value().mm[vid];
	     	  if (value().edges[j] == vid)
	     	  {
	     	  	value().tri[j].push_back(value().mm[vedges[0]]);
	     	  }
	     }
	     
	     for (int i = 0; i < value().edges.size(); ++ i)
	     {
	     	value().triangleNum[i] = value().tri[i].size();
	     }
	}
	else vote_to_halt();
	
	
	
    }
};


//worker
class triWorker : public Worker<triVertex> {
    char buf[100];

public:
    //C version
    virtual triVertex* toVertex(char* line)
    {
        triVertex* v = new triVertex;
        istringstream ssin(line);
        ssin >> v->id;
        int num;
        ssin >> num;
        v->value().K = -1;
        for (int i = 0; i < num; i++) {
            int nb;
            ssin >> nb;
            v->value().edges.push_back(nb);
        }
        //new
        v->value().check = 0;
        v->value().mySet.resize(v->value().edges.size());
        v->value().ans.resize(v->value().edges.size());
        v->value().tri.resize(v->value().edges.size());
        v->value().done.resize(v->value().edges.size());
        for (int i = 0; i < v->value().edges.size(); ++ i) v->value().done[i] = 0;
        v->value().triangleNum.resize(v->value().edges.size());
        sort(v->value().edges.begin(), v->value().edges.end());//may need cmp

	for (int i = 0; i < v->value().edges.size(); ++ i) v->value().mm[v->value().edges[i]] = i;
	
        return v;
    }

    virtual void toline(triVertex* v, BufferedWriter& writer)
    {
    	/*
        sprintf(buf, "%d\n", v->id);
        writer.write(buf);
        //sprintf(buf, "hello");

        for (int i = v->value().edges.size()-1; i >= 0; --i)
        {
            if (v->value().edges[i] > v->id){
            sprintf(buf, "-%d %d\n", v->value().edges[i], v->value().triangleNum[i]);
            writer.write(buf);
            }
            else
            break;
        }
        */
        for (int i = v->value().edges.size()-1; i >= 0; --i)
        {
        	if (v->value().edges[i] > v->id)
        	{
        		sprintf(buf, "%d %d\t%d ", v->id, v->value().edges[i], v->value().triangleNum[i] );
        		writer.write(buf);
        		for (int j = 0; j < v->value().triangleNum[i]; ++ j)
        		{
        			sprintf(buf, "%d ", v->value().edges[v->value().tri[i][j]]);
        			writer.write(buf);
        		}
        		sprintf(buf, "\n");
        		writer.write(buf);
        	}
        	else break;
        }
    }
};

void pregel_tri(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    //triAgg agg;
    triWorker worker;
    //worker.setAggregator(&agg);
    worker.run(param);
}
