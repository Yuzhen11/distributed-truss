#include "basic/pregel-dev.h"
#include "utils/type.h"
#include <sstream>
#include <set>
#include <map>
using namespace std;
 
struct edgeValue {
	vector<int> tri;
	int ans;
	map<int, int> mm;
	
	vector<int> est;
	int sup;
	bool sendm;
};
const int inf = 100000000;

ibinstream& operator<<(ibinstream& m, const edgeValue& e)
{
    m << e.tri;
    m << e.ans;
    m << e.mm;
    m << e.est;
    m << e.sup;
    m << e.sendm;
    return m;
}

obinstream& operator>>(obinstream& m, edgeValue& e)
{
    m >> e.tri;
    m >> e.ans;
    m >> e.mm;
    m >> e.est;
    m >> e.sup;
    m >> e.sendm;
    return m;
}
//---------------------------
//------------------------------

//====================================

//vertex
class trussEdge : public Vertex<intpair, edgeValue, int, IntPairHash> {
public:

    virtual void compute(MessageContainer& messages)
    {
    	if (step_num() == 1)
    	{
    		//initialize
    		value().est.resize(value().tri.size());
    		for (int i = 0; i < value().tri.size(); ++ i) value().est[i] = 1;
    		sort(value().tri.begin(), value().tri.end());
        	value().sup = value().tri.size();
        	for (int i = 0; i < value().tri.size(); ++ i) value().mm[value().tri[i]] = i;
   		value().sendm = 0;
    		
    		if (value().sup == 0) value().ans = 2;
    		else if (value().sup > 0) value().ans = -1;
    	}
    	else if (value().ans == -1)
    	{
    		//receive
    		for (int i = 0; i < messages.size(); ++ i)
    		{
    			int tmp = value().mm[messages[i]];
    			if (value().est[tmp] == 1)
    			{
    				value().est[tmp] = 0;
    				value().sup --;
    			}
    		}
    		
    		int globalK = *((int*)getAgg());
    		//send
    		bool send = 0;
    		if (value().sup <= globalK)
    		{
    			value().ans = globalK + 2;
    			for (int i = 0; i < value().tri.size(); ++ i)
    			{
    				send = 1;
    				int tmp = value().tri[i];
    				
    				intpair to(min(id.v1, tmp), max(id.v1, tmp));
    				send_message(to, id.v2);
    				
    				intpair to2(min(id.v2, tmp), max(id.v2, tmp));
    				send_message(to2, id.v1);
    			}
    		}
    		if (send) value().sendm = 1;
    		else value().sendm = 0;
    			
    	}
    	else vote_to_halt();
	
    }
};
class trussAgg : public Aggregator<trussEdge, bool, int> {
private:
    int K;
    bool allLessK;

public:	
	
    virtual void init()
    {
	if (step_num() == 1) 
	K = 0;
	else K = *((int*)getAgg());
	allLessK = 1;
	
	//cout << K << endl;
    }

    virtual void stepPartial(trussEdge* v)
    {
	if (v->value().sendm == 1) allLessK = 0;
    }

    virtual void stepFinal(bool* part)
    {
    	 allLessK = *part && allLessK;
    }

    virtual bool* finishPartial()
    {
	 return &allLessK;
    }
    virtual int* finishFinal()
    {	
	 if (allLessK) K++;
	 return &K;
    }
};

//worker
class trussWorker : public Worker<trussEdge, trussAgg> {
    char buf[100];

public:
    //C version
    virtual trussEdge* toVertex(char* line)
    {
        trussEdge* v = new trussEdge;
        istringstream ssin(line);
        int from, to;
        ssin >> from >> to;
        v->id.v1=from;
        v->id.v2=to;
        
        int num;
        ssin >> num;
        //v->value().K = -1;
        for (int i = 0; i < num; i++) {
            int nb;
            ssin >> nb;
            v->value().tri.push_back(nb);
        }
        
        return v;
    }

    virtual void toline(trussEdge* v, BufferedWriter& writer)
    {
    	sprintf(buf, "%d %d %d\n", v->id.v1, v->id.v2, v->value().ans);
    	writer.write(buf);
    }
};

void pregel_tri(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    trussAgg agg;
    trussWorker worker;
    worker.setAggregator(&agg);
    worker.run(param);
}
