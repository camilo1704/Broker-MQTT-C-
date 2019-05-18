#include <string>
#include <thread>
#include <mutex>
#include <set>
#include <vector>
#include <iostream>
#include <chrono>
#include <condition_variable>
#include <array>
#include <queue>
#include <algorithm>
#include <stdexcept>
using namespace std::chrono;
using namespace std;
using TopicName = string;
using TopicValue = string;
 enum class Type { CONNECT, PUBLISH, CONNACK, SUBSCRIBE, UNSUBSCRIBE, DISCONNECT };

template<class T, size_t N>
class Queue {

array<T,N> arr;
mutable mutex m;
condition_variable c;

public:
int inicio=0;
int fin=0;
void put(const T& t){
    lock_guard<mutex> lock(m);
    fin+=1;
    arr[fin]=t;
    c.notify_one();

};
void get(T* t){				// lo saco en t
    unique_lock<mutex> lock(m);
    while(arr.empty()){ c.wait(lock);};// espurios
    *t = arr[inicio]; 
    inicio+=1;}
T operator[](size_t i){return arr[i];};
~Queue(){for(size_t i=0;i<N;++i) delete arr[i];};
};

class Message {
public:
    enum class Type { CONNECT, PUBLISH, CONNACK, SUBSCRIBE, UNSUBSCRIBE, DISCONNECT };
    Type getType()const {return type;};
    virtual Message *clone() = 0; // para evitar object splicing
    Message(Type tipo):type(tipo){};
    virtual ~Message(){};
private:
    Type type;
};

class ConnectMsg : public Message {
private:
    string username;
    string password;
    ConnectMsg(ConnectMsg &tocopy)=delete;
    ConnectMsg(ConnectMsg &&tocopy)=delete;
public:
    ConnectMsg( string usuario, string contrasenia):Message(Type::CONNECT), username(usuario),password(contrasenia){};
    Message *clone(){return new ConnectMsg(username,password);};
    string getuser(){return username;};
    string getpassword(){return password;};
};
class ConnAckMsg : public Message {
public:
    enum class Status { CONNECTION_OK, LOGIN_ERROR };
    ConnAckMsg(Status estado):Message(Type::CONNACK),status(estado){};
    Message *clone(){return new ConnAckMsg(status);};
    Status getStatus(){return status;};

private:
    Status status;
};


class PublishMsg : public Message {
private:
    TopicName topic;
    TopicValue value;
    bool retain;
public:
    PublishMsg(TopicName &topico, TopicValue &valor,bool retener):Message(Type::PUBLISH),topic(topico),value(valor),retain(retener){};
    Message *clone(){ return  new PublishMsg(topic,value,retain);}
    TopicName getTopic(){return topic;}
    TopicValue getValue(){return value;}
    bool Retain(){return retain;}

// ...
};
class SubscribeMsg : public Message {
private:
    TopicName topic;
public:
    SubscribeMsg(TopicName &topico):Message(Type::SUBSCRIBE),topic(topico){};
    Message *clone(){ return new SubscribeMsg(topic);}
    TopicName getTopic(){return topic;}

};
class UnsubscribeMsg : public Message {
private:
    TopicName topic;
public:
      UnsubscribeMsg(TopicName &topico):Message(Type::SUBSCRIBE),topic(topico){};
      TopicName getTopic(){return topic;}
      Message* clone(){return new UnsubscribeMsg(topic);};
};
class DisconnectMsg : public Message {
public:
    DisconnectMsg():Message(Type::DISCONNECT){};
    Message* clone(){return new DisconnectMsg;};
};

class ClientOpsIF {
public:
    virtual void recvMsg(const Message &) = 0;
};

class BrokerOpsIF {
public:
virtual void sendMsg(const Message &) = 0;
};


class Client;
struct Subscription {
    TopicName topic;
    Client *owner;
};
struct RetainedTopic {
    TopicName topic;
    TopicValue value;
    Client *owner;
};

class Client : public BrokerOpsIF {
private:
thread th;
static vector<string> contrasenias;//contraseñas para comparar
static vector<string> usuarios;
ClientOpsIF *cif; // para mandarle mensajes al cliente
    static const size_t t=10;
    Queue <Subscription *,t> subscriptions;
    Queue <RetainedTopic *,t> topics;
    Queue<Message*,t > recvQueue; //para publicar 
    Queue<Message*,t> recvQueue_tothreat;

public:
    Client(ClientOpsIF * c):cif(c){};
    ClientOpsIF* returncif(){return cif;};
    ~Client(){th.join();};
    void runthread(){ while(recvQueue_tothreat.inicio!=recvQueue.fin){

                     switch (recvQueue_tothreat[recvQueue.inicio]->getType()) {
                     case Message::Type::CONNECT:{
                         ConnectMsg* m;
                         recvQueue_tothreat.get((Message**)&m);
                         ConsumMsg(*((Message*)m));
                         break;}
                      case Message::Type::PUBLISH:{
                         PublishMsg* m;
                         recvQueue_tothreat.get((Message**)&m);
                         ConsumMsg(*((Message*)m));}
                      case Message::Type::SUBSCRIBE:{
                         SubscribeMsg* m;
                         recvQueue_tothreat.get((Message**)&m);
                         ConsumMsg(*((Message*)m));}
                      case Message::Type::UNSUBSCRIBE:{
                         UnsubscribeMsg* m;
                          recvQueue_tothreat.get((Message**)&m);
                          ConsumMsg(*((Message*)m));}
                      case Message::Type::DISCONNECT:{
                         DisconnectMsg* m;
                          recvQueue_tothreat.get((Message**)&m);
                          ConsumMsg(*((Message*)m));}

                     }
}
                    };
    RetainedTopic* getRetained(){
                            RetainedTopic* n=new RetainedTopic;
                            topics.get(&n);
                            return n;
    };

    Subscription* getSubscription(){
                            Subscription* s=new Subscription;
                            subscriptions.get(&s);
                            return s;

    };
    Message* getMessage(){ Message* m;
                           recvQueue.get(&m);
                           return m;};
    bool emptyretained(){return topics.fin==topics.inicio;};
    bool emptysubscriptions(){return subscriptions.fin==subscriptions.inicio;};
    bool emptyqueue(){return recvQueue.fin==recvQueue.inicio;} //retorna 1 si esta vacia (si no hay nada que publicar)

    void sendMsg(const Message &m) {
        recvQueue_tothreat.put(const_cast<Message*>(&m)->clone());   

    };

    void ConsumMsg(Message &m){


        //se fija que tipo de mensaje es, lo clona y lo guarda en la cola si es publish o un/subscribe y retained



           if (m.getType()==Message::Type::CONNECT){           
               vector<string>::iterator it1;
               vector<string>::iterator it2;
               bool connected=true;
                   it1=find(contrasenias.begin(),contrasenias.end(),((ConnectMsg*)&m)->getpassword());
                   if (it1==contrasenias.end())
                   {    connected=false;
                   }
                   it2=find(usuarios.begin(),usuarios.end(),((ConnectMsg*)&m)->getuser());
                   if(it2==usuarios.end())
                       connected==false;
                   if(connected){
                           ConnAckMsg to_send(ConnAckMsg::Status::CONNECTION_OK); //envia el CONNACK
                           cif->recvMsg(to_send);}


                   else
                   {
                           ConnAckMsg to_send(ConnAckMsg::Status::LOGIN_ERROR);
                           cif->recvMsg(to_send);
                           DisconnectMsg* disc;
                           recvQueue.put(disc);      //que borre el cliente que creo si no se loggea correctamente

                   }

        }
          if (m.getType()==Message::Type::SUBSCRIBE){
               // recvQueue.put(((SubscribeMsg*)&m)->clone());
                Subscription* s=new Subscription;
                s->owner=this;                              //guarda el owner q le sirve cuando el broker se lo copie a su container
                s->topic=((SubscribeMsg*)&m)->getTopic();  //lo agrega a subscriptions


           }
           if (m.getType()==Message::Type::PUBLISH){
                recvQueue.put(((PublishMsg*)&m)->clone());//hay que publicarlo a los que le interese, entonces lo pongo en la cola
                if ((((PublishMsg*)&m))->Retain()==true){ //se fija si hay q retenerlo y lo guarda
                RetainedTopic* t=new RetainedTopic;
                t->owner=this;
                t->topic=((PublishMsg*)&m)->getTopic();
                t->value=((PublishMsg*)&m)->getValue();
               topics.put(t);
                }
           }
           if (m.getType()==Message::Type::UNSUBSCRIBE){//lo pongo en subscriptions y el broker ve que ya estaba subscripto a eso y unsubscribe

               Subscription* s=new Subscription;
               s->owner=this;
               s->topic=((SubscribeMsg*)&m)->getTopic();
               subscriptions.put(s);
           }
           if (m.getType()==Message::Type::DISCONNECT){
              //Guardo un mensaje como nullptr en la cola y eventualmente cuando el broker lo lee borra ese cliente
                 DisconnectMsg* m;
                 recvQueue.put(m);
           }
   }
  };



vector<string> Client::contrasenias={"hola1234","holahola","hholala","holaHOLA"};
vector<string> Client::usuarios={"User1", "User2","User3","User4"};

class SimPublisher;

//creo funciones de comparacion de topics para clase subscribe y retainedtopic
struct compare_topics_subs{
    bool operator()( const Subscription* s1, const Subscription* s2) const{
        return s1->topic<s2->topic;
    }
};
struct compare_topics_ret
{
    bool operator()(const RetainedTopic* r1, const RetainedTopic* r2)const {
        return r1->topic<r2->topic;
    }
};



class Broker {

static Broker* instance;
Broker(){};
Broker(Broker &)=delete;
Broker(Broker &&)=delete;
vector<Client*> clientes;
multiset<Subscription*,compare_topics_subs> subscripciones; //ordeno por topicos con compare_topics
multiset<RetainedTopic*,compare_topics_ret> topicos_retenidos;

public:
~Broker(){vector<Client*>::iterator it;
          for(it=clientes.begin();it!=clientes.end();++it) delete *it;
          multiset<Subscription*,compare_topics_subs>::iterator it1;
                for(it1=subscripciones.begin();it1!=subscripciones.end();++it1) delete *it1;
          multiset<RetainedTopic*,compare_topics_ret>::iterator it2;
                for(it2=topicos_retenidos.begin();it2!=topicos_retenidos.end();++it2) delete *it2;
           };
BrokerOpsIF* registerClient(ClientOpsIF * cliente){

     Client* newcli= new Client(cliente); //creo cliente nuevo
     clientes.push_back(newcli); //inserto cliente en lista de broker
     return newcli;           // retorno el puntero a simX
};

    static Broker* getInstance(){ if (instance == 0)
        {
            instance = new Broker();
        }
        return instance;
}
    void publicMsg( Message* m, Client* c);
    void copyretained(Message* m, Client * const c){

      {                                               //armo el retain topic, lo guardo en la lista de broker (se borra del client al salir del scope de esta funcion)
           RetainedTopic* t =new RetainedTopic;
           t->topic=((PublishMsg*)m)->getTopic();
           t->value=((PublishMsg*)m)->getValue();
           t->owner=c;
           topicos_retenidos.insert(t);
        }
    };
    void verifysubscription(SubscribeMsg* s, Client* const c){
        //busco el topic y veo si hay un owner con una subscription a ese topic, si es asi entonces es un unsubscribe
         typedef multiset<Subscription*>::iterator It;
         Subscription* to_compare=new Subscription;
         to_compare->topic=s->getTopic();
         std::pair<It,It> ret =subscripciones.equal_range(to_compare);
         for(It it=ret.first;it!=ret.second;++it){
                if ((*it)->owner==c)
                    subscripciones.erase(it);
         }

    };


    void runBroker(std::chrono::milliseconds ms){
        std::chrono::time_point<std::chrono::system_clock> end;

            end = std::chrono::system_clock::now() + ms; // tiempo final

            while(std::chrono::system_clock::now() < end) // es tiempo final?
            {
               //cada cierto tiempo entrar en los clients
                if(!clientes.empty())
                    for(vector<Client*>::iterator it=clientes.begin();it!=clientes.end();++it){ //recorre cada client y ve q hay de nuevo
                                if ((*it)->emptyqueue()==false){
                                    publicMsg((Message*)(*it)->getMessage(),*it);// cuando hace el get de la queue se "elimina" pq inicio+1
                                }
                                if ((*it)->emptyretained()==false){
                                    copyretained((Message*)(*it)->getRetained(),*it); //cuando hace el get de la queue se "elimina" pq inicio+1
                                    }
                                if ((*it)->emptysubscriptions()==false)
                                    verifysubscription((SubscribeMsg*)(*it)->getSubscription(),*it); // si ya estaba subscripto es un unsubscribe

                }
            }

}

};

Broker* Broker::instance = 0;

class SimClient : public ClientOpsIF {
thread simth;
Broker &broker;
bool logged=false;
virtual void runSim() = 0;
public:
SimClient(Broker &b):broker(b){};
void start();
void setlogged(bool b){logged=b;}
bool getlogged(){return logged;}
Broker* getbroker(){return &broker;}
virtual void recvMsg(Message* m){ }
virtual ~SimClient(){simth.join();};

};


void SimClient::start() // En pseudo C++
{
    simth = move(thread{&SimClient::runSim, this});
}
// Ejemplo de un cliente Publisher


class SimPublisher : public SimClient {
    void runSim();

public:
    SimPublisher(Broker& b):SimClient(b){};
    ~SimPublisher(){};
void recvMsg(const Message & m)

{cout<<"Mensaje recibido"<<endl;
    if(m.getType()==Message::Type::CONNACK){
        if((((ConnAckMsg*)&m)->getStatus()==ConnAckMsg::Status::CONNECTION_OK))
            setlogged(true);
          }

};

};
//cliente Suscriber

void SimPublisher::runSim()
{
    BrokerOpsIF *brops = getbroker()->registerClient(this);
    brops->sendMsg(ConnectMsg{"User1", "hola1234"});

    for(int i=0;i<10;++i) {
        this_thread::sleep_for( 10*(chrono::microseconds)rand()/(RAND_MAX+1.0) );
        PublishMsg*  m;
       // fill m;
        brops->sendMsg(*(m->clone())); // PUBLISH
    }
    brops->sendMsg(DisconnectMsg{}); // DISCONNECT
}

class SimSubscriber: public SimClient{
void runSim();
public:
SimSubscriber(Broker &b):SimClient(b){};
~SimSubscriber(){};
void recvMsg(const Message & m){
        cout<<"Mensaje recibido"<<endl;
        if((((ConnAckMsg*)&m)->getStatus()==ConnAckMsg::Status::CONNECTION_OK))
            setlogged(true);
            };
};
void SimSubscriber::runSim()
{
    BrokerOpsIF *brops = getbroker()->registerClient(this);
    brops->sendMsg(ConnectMsg{"user", "pass"});

    for(int i=0;i<10;++i) {
        this_thread::sleep_for( 10*(chrono::microseconds)rand()/(RAND_MAX+1.0) );
        SubscribeMsg*  m;
     //   fill m;
        brops->sendMsg(*(m->clone())); // PUBLISH
    }
    brops->sendMsg(DisconnectMsg{}); // DISCONNECT
}

void Broker::publicMsg( Message* m, Client* c){
    //verifico que subscriptores en la lista del broker les interesa
    typedef multiset<Subscription*>::iterator It;
    Subscription* s;
    s->topic=((SubscribeMsg*)m)->getTopic();
    std::pair<It,It> ret = subscripciones.equal_range(s);
    //entro a cada cliente y lo mando a simclient a traves de *cif q es un ClientOpsIF
    for (It it=ret.first; it!=ret.second; ++it)
    {
     SimPublisher* s= (SimPublisher *)(((*it)->owner)->returncif());
     s->recvMsg(*m);
                     }

    if(m->getType()==Message::Type::DISCONNECT){ // si se desconecta, borra al client del broker y sus susbcripciones y llamo a su destructor
        for(multiset<Subscription*>::iterator ite=subscripciones.begin();ite!=subscripciones.end();++ite){
            if ((*ite)->owner==c){
                subscripciones.erase(ite);
            }
        }
        for(vector<Client*>::iterator it=clientes.begin();it!=clientes.end();++it){
            if(*it==c){
                clientes.erase(it);
                c->~Client();    //definir el destructor bien
            }
        }
   }
};

int main()
{
    std::chrono::milliseconds ms(3000);
     Broker* b=Broker::getInstance();
     b->runBroker(ms);
     SimPublisher Publisher1(*b);
     Publisher1.start();
     //SimSubscriber Subscriber1(*b);
     //Subscriber1.start();
     SimPublisher Publisher2(*b);
     Publisher2.start();

    return 0;
}
