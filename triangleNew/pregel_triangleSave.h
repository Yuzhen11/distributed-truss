#include "basic/pregel-dev.h"
#include <sstream>
using namespace std;
 
struct triValue {
    int K;
    vector<VertexID> edges;
    vector<int> triangleNum;
};


ibinstream& operator<<(ibinstream& m, const triValue& v)
{
    m << v.K;
    m << v.edges;
    m << v.triangleNum;
    return m;
}

obinstream& operator>>(obinstream& m,triValue& v)
{
    m >> v.K;
    m >> v.edges;
    m >> v.triangleNum;
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

//vertex
class triVertex : public Vertex<VertexID, triValue, edgeValue> {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() == 1)
	{
	     
	     for (int i = 0; i < value().edges.size(); ++ i)
	     {
	     	  send_message(value().edges[i], edgeValue(id, value().edges));	
	     }
	}
	else if (step_num() == 2)
	{
	     for (int i = 0; i < messages.size(); ++ i)
	     {
	     	  VertexID& vid = messages[i].v;
	     	  vector<VertexID>& vedges = messages[i].edges;
	     	  
	     	  for (int j = 0; j < value().edges.size(); ++ j)
	     	  {
	     	  	if (value().edges[j] == vid)
	     	  	{
	     	  	     int p1 = 0;
	     	  	     int p2 = 0;
	     	  	     int ans = 0;
	     	  	     while(p1 < value().edges.size() && p2 < vedges.size())
	     	  	     {
	     	  	     	if (value().edges[p1] == vedges[p2]) {ans ++; p1++; p2++; }
	     	  	     	else if (value().edges[p1] < vedges[p2]) {p1++;}
	     	  	     	else p2++;
	     	  	     }
	     	  	     value().triangleNum[j] = ans;
	     	  	     break;
	     	  	}
	     	  }
	     }	
	}
	else vote_to_halt();
    }
};

//aggregator
/*
class triAgg : public Aggregator<triVertex, bool, int> {
private:
    int K;
    bool allLessK;

public:
    virtual void init()
    {

    }

    virtual void stepPartial(triVertex* v)
    {

    }

    virtual void stepFinal(bool* part)
    {
    }

    virtual bool* finishPartial()
    {

    }
    virtual int* finishFinal()
    {

    }
};
*/
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
        v->value().triangleNum.resize(v->value().edges.size());
        sort(v->value().edges.begin(), v->value().edges.end());//may need cmp
        return v;
    }

    virtual void toline(triVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\n", v->id);
        writer.write(buf);
        //sprintf(buf, "hello");

        for (int i = 0; i < v->value().edges.size(); ++ i)
        {
            sprintf(buf, "-%d %d\n", v->value().edges[i], v->value().triangleNum[i]);
            writer.write(buf);
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
