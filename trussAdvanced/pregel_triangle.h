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
	vector<int> p;
};
const int inf = 100000000;

ibinstream& operator<<(ibinstream& m, const edgeValue& e)
{
    m << e.tri;
    m << e.ans;
    m << e.mm;
    m << e.p;
    return m;
}

obinstream& operator>>(obinstream& m, edgeValue& e)
{
    m >> e.tri;
    m >> e.ans;
    m >> e.mm;
    m >> e.p;
    return m;
}
//---------------------------
struct messageValue
{
    intpair ip;
    
    messageValue(){}
    messageValue(intpair newIp):ip(newIp){}
};
ibinstream& operator<<(ibinstream& m, const messageValue& e)
{
    m << e.ip;
    return m;
}

obinstream& operator>>(obinstream& m,messageValue& e)
{
    m >> e.ip;
    return m;
}
//------------------------------

//====================================

//vertex
class trussEdge : public Vertex<intpair, edgeValue, messageValue, IntPairHash> {
public:

    int subfunc(trussEdge * e)
    {
    	int& K = e->value().ans;
	vector<int> cd(K+2, 0);
	for (int i = 0; i < e->value().tri.size(); ++ i)
	{
		cd[min(e->value().p[i], K)] ++;
	}
	for (int i = K; i >= 1; --i)
	{
		cd[i] += cd[i+1];
		if (cd[i] >= i-2)
    		return i;
	}
    }

    virtual void compute(MessageContainer& messages)
    {
    	if (step_num() == 1)
    	{
    		//initialize
    		sort(value().tri.begin(), value().tri.end());
	        value().ans = value().tri.size() + 2;
       		for (int i = 0; i < value().tri.size(); ++ i) value().mm[value().tri[i]] = i;
       		value().p.resize(value().tri.size());
    		for (int i = 0; i < value().tri.size(); ++ i) value().p[i] = inf;
    		
    		for (int i = 0; i < value().tri.size(); ++ i)
    		{
    			int tmp = value().tri[i];
    			intpair to(min(id.v1, tmp), max(id.v1, tmp));
    			intpair mess(id.v2, value().ans);
    			send_message(to, mess);
    			
    			intpair to2(min(id.v2, tmp), max(id.v2, tmp));
    			intpair mess2(id.v1, value().ans);
    			send_message(to2, mess2);
    		}
    	}
    	else
    	{
    		for (int i = 0; i < messages.size(); ++ i)
    		{
    			int& w = messages[i].ip.v1;
    			int& newP = messages[i].ip.v2;
    			
    			int j = value().mm[w];
    			if (newP < value().p[j]) 
    				value().p[j] = newP;
    		}
    		int x = subfunc(this);
    		if (x < value().ans)
    		{
    			value().ans = x;
    			for (int i = 0; i < value().tri.size(); ++ i)
    			{
    				if (value().ans < value().p[i])
    				{
	    				int tmp = value().tri[i];
		    			intpair to(min(id.v1, tmp), max(id.v1, tmp));
		    			intpair mess(id.v2, value().ans);
		    			send_message(to, mess);
		    			
		    			intpair to2(min(id.v2, tmp), max(id.v2, tmp));
		    			intpair mess2(id.v1, value().ans);
		    			send_message(to2, mess2);
		    		}
    			}
    		}
    	}
	vote_to_halt();
	
    }
};


//worker
class trussWorker : public Worker<trussEdge> {
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
    
    trussWorker worker;
    
    worker.run(param);
}
