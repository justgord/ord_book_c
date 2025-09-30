//
//  chunks.c
//            
#include <time.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

typedef uint32_t U32;
typedef uint64_t U64;

const U32 seed=0x3134214;


U32 rng_u=0x313e2f4;
U32 rng_v=0x67adbb4;
U32 randn(U32 N)
{
    // marsaglias pseudo rng

    rng_v = 36969*(rng_v & 65535) + (rng_v >> 16);
    rng_u = 18000*(rng_u & 65535) + (rng_u >> 16);
    U32 r = (rng_v << 16) + (rng_u & 65535);

    return r % N;
}


#define _count_of(rg)  ( sizeof(rg) / sizeof(rg[0]) )


// ORDER FLAGS / TYPES

typedef enum {
    BUY             = 1 << (0),
    SEL             = 1 << (1),
    LIMIT           = 1 << (2),
    STOP_LOSS       = 1 << (3),
    TAKE_PROFIT     = 1 << (4),  // ? == STOP | LIMIT | 
    MAKER           = 1 << (5),  //?
    ALL_OR_NONE     = 1 << (6),
    IMM_OR_CANCEL   = 1 << (7), 
} ORD_TYP;


typedef struct THEAD
{
    struct THEAD    *nxt;       // next header / chunk in single linked list

    U32             nsk;        // n skip = # empty slots at front
    U32             n;          // n used entries [ NCHUNK-n free slots ]

} THEAD;
typedef struct THEAD *PHEAD;


typedef struct TORDER
{   
    ORD_TYP typ;                // 32  4
    U32     qty;                // 32  4
    U64     oid;                // 64  8
                                //    16 bytes [ cacheline 64bytes => 4 orders per cacheline ]
                                //              L1 32k d > L2 256 i/d > 8M i/d => ~1k ords in cache
} TORDER;                               
typedef struct TORDER *PORDER;
typedef TORDER ITEM;            // alias .. for when we want to template / macro this stuff


typedef struct TSLOT
{
    PHEAD   fr;
    PHEAD   bk;
} TSLOT;
typedef struct TSLOT  *PSLOT;


///

const U64 NCHUNK    = (1 << 5) - 1;               // num data slots per chunk [ small for testing ]

const U32 PRIC      = 5000;

U64     nordids=0;
U32     nallocs=0;

PHEAD   freelist=NULL;                  // free list of chunks for reuse .. one per type


PHEAD chunk_alloc()
{
    size_t sz=0;
    sz += sizeof(THEAD);
    sz += NCHUNK*sizeof(TORDER);

    PHEAD p = malloc(sz);
    memset(p,0,sz);

    nallocs++;

    return p;
}

void chunk_rel(PHEAD chk)           //nb. assumes its unlinked already
{
    if (!chk)
        return;

    chk->nxt = NULL;
    chk->n=0;

    chk->nxt = freelist; 
    freelist=chk; 
}

PHEAD chunk_get()           // get off free list, or alloc
{
    PHEAD tos=freelist;
    if (tos)
    {
        freelist=tos->nxt;
        tos->nxt = NULL;
        tos->n=0;
        tos->nsk=0;
        return tos;
    }
    return chunk_alloc();    // none, alloc a new one
}


void show_chain(PHEAD chk)
{
    printf("  show chain : \n");

    if (!chk)
        return;

    PHEAD p=chk;
    while(p)
    {
        printf("    chunk : n=%u\n", p->n);
        p = p->nxt;
    }
}


ITEM* chunk_item(PHEAD chk)
{
    // set aside space for an item at end of last chunk, return pointer to it [ for data ]

    assert(chk);

    PHEAD pen=chk;
    while(pen->nxt)
        pen=pen->nxt;

    if (pen->n >= NCHUNK)
    {
        //printf("  chunk_item : new\n");
        pen = chunk_get();
        chk->nxt = pen;
    }

    assert(pen->n < NCHUNK);            // space at end of this chunk
    
    ITEM* p = (ITEM*)(pen+1);           // chk -> HEADER ITEM ITEM ... 
    p += pen->nsk;
    p += pen->n;

    pen->n++;

    return p;
}


ITEM* chunk_new_order(PHEAD chk, U32 otyp, U32 qty, U32 pri)
{
    PORDER p = chunk_item(chk);

    p->typ = otyp;
    p->qty = qty;
    p->oid = ++nordids;

    return p;
}

void rand_order(PHEAD chk)
{
    U32 qty = 1+randn(10)+randn(10)+randn(10);

    U32 otyp = (randn(2)==1) ? BUY : SEL;

    //printf("  rand_order : %s %5u @ %u\n", ((otyp & BUY)?"B":"S"), qty, PRIC);
    printf("  ord : %s %3u \n", ((otyp & BUY)?"B":"S"), qty);

    PORDER po = chunk_new_order(chk, otyp, qty, PRIC);
}

ITEM* chunk_append_copy(PHEAD chk, PORDER p)
{
    ITEM* pc = chunk_item(chk);

    pc->oid=p->oid;
    pc->typ=p->typ;
    pc->qty=p->qty;

    return pc;
}

void slot_append_order(PSLOT pslot, U32 tp, U32 qty)
{
    PHEAD pen=pslot->bk;

    if ((pen->nsk + pen->n ) >= NCHUNK)
    {   
        pen = chunk_get();
        pslot->bk->nxt = pen;
        pslot->bk = pen;
    }

    assert((pen->nsk + pen->n ) < NCHUNK);

    ITEM* p = (ITEM*)(pen+1); 
    p += pen->nsk;
    p += pen->n;

    p->typ = tp;
    p->qty = qty;
    p->oid = ++nordids;

    pen->n++;

    assert(pen);
    assert((pen->nsk+pen->n)<=NCHUNK);
    assert(pen->nsk<NCHUNK);
}

PHEAD scan_match_order(PSLOT pslot,  U32 tp, U32 qty, U32 pri)    // match or append new order
{
    // starting from front, we might have to consume lots of chunks if its a big order

    assert(pslot && pslot->fr && pslot->bk);

    PHEAD chk=pslot->fr;
    assert(chk);

    //printf("  >  %s %3u %6u\n", ((tp & BUY)?"B":"S"), qty, pri);

    // if book slot is empty, or book order is same type .. put order at end of Q

    if (0==(chk->n))
    {
        // empty

        slot_append_order(pslot, tp, qty);
        return NULL;
    }

    ITEM* p0 = (ITEM*)(chk+1);
    ITEM* pf = p0+chk->nsk;
    ORD_TYP tp0 = pf->typ;
    if ((tp0 & BUY) == (tp & BUY))  
    {
        // same type

        slot_append_order(pslot,tp, qty);
        return NULL;
    }

    // consume matching B-S until we have qty or used up incoming or book orders

    PHEAD trades = chunk_get();
    ITEM* pm = chunk_new_order(trades, tp, qty, pri);       

    U32 qtot=0;
    ITEM* pt=NULL;      // trade order
    ITEM* p =NULL;      // book order
    U64 toid;
    int stop=0;
    while(!stop)
    {
        U32 nb=chk->nsk;
        U32 ne=nb+chk->n;

        p0 = (ITEM*)(chk+1);
        for (U32 k=nb;k<ne && qtot<qty ;k++)
        {
            p = p0+k;
            qtot += p->qty;
            
            //printf("    match : %s %3u \n", ((p->typ & BUY)?"B":"S"), p->qty);

            pt = chunk_append_copy(trades, p);

            toid=p->oid;

            p->oid=0;
            p->typ=0;
            p->qty=0;

            chk->n--;
            chk->nsk++;
        }

        if (qtot>=qty)
            stop=1;
        else if (!chk->nxt)
            stop=1;
        else
            chk = chk->nxt; 
    }

    assert(chk);

    // handle last partial chunk left over, if any

    //printf("    tot   :  %4u \n", qtot);

    if (qtot<qty)
    {
        // incoming order partially remains

        U32 dq=qty-qtot;
        assert(chk && (chk->n)<NCHUNK);
        p->oid=pm->oid;
        p->typ=tp;
        p->qty=dq;
        chk->n++;
        chk->nsk--;
        //printf("  left    : %s %3u\n", ((tp & BUY)?"B":"S"), dq);
    }
    else if (qtot>qty)
    {
        // book order partially remains

        U32 dq=qtot-qty;
        assert(chk && (chk->n)<NCHUNK);
        p->oid=toid;
        p->typ=tp0;
        p->qty=dq;
        chk->n++;
        chk->nsk--;
        //printf("  left    : %s %3u\n", ((tp0 & BUY)?"B":"S"), dq);
    }
    else
    {
        //printf("  left    : --\n");
        if (!chk->n)
            chk->nsk=0;
    }

    // remove front empty chunks .. and rewrite front of chunk list Q if changed

    chk=pslot->fr;
    while(chk->nxt && 0==(chk->n))
    {
        pslot->fr=chk->nxt;
        chunk_rel(chk);
        chk=pslot->fr;
    }
    assert(chk);

    return trades;
}


void trace_chunk(PHEAD chk)
{
    while(chk)              
    {
        U32 nb=chk->nsk;
        U32 ne=nb+chk->n;

        printf("            : ");

        ITEM* p0 = (ITEM*)(chk+1);
        for (U32 k=0;k<NCHUNK;k++)
        {
            ITEM* p=p0+k;

            if (p->typ)
                printf("  %s %3u",((p->typ & BUY)?"B":"S"), p->qty);    
            else
                printf("  ---  ");
        }
 
        printf(" : %u %u \n",chk->nsk,chk->n);
        chk = chk->nxt;
    }
}


typedef struct TBS
{
    ORD_TYP     typ;
    U32         qty;
} TBS;


U64 ns_cnts[32]={0};
U64 ns_ncnt=0;

const int SBIN = 200;

int lat_bin(int i)
{
    return round(SBIN*pow(1.25, i));
}

void latency_bins()
{
    for (int i=0;i<32;i++) 
    {
        int msi = lat_bin(i);
        printf("bin : %d : %d\n",i,msi);
    }
}

void latency_log(U64 ns)
{
    for (int i=31;i>=0;i--)
    {
        int nsmax = lat_bin(i);
        if (ns>=nsmax)
        {
            ns_cnts[i]++; 
            ns_ncnt++;
            return;
        }
    }
    ns_cnts[0]++;
}

void latency_trace()
{
    printf("latency    : \n");
    printf("               cnt     > ns      pct  %%\n");
    int tt=0;                       // flag to omit top 0 counts
    for (int i=31;i>=0;i--)
    {
        int nsmax = lat_bin(i);
        U64 n = ns_cnts[i];
        if (n>0 || tt) 
        {
            printf("          %8lu %8d  %8.2f %%\n",n, nsmax, (100.0*n)/ns_ncnt);
            tt++;
        }
    }
}


void test_run(TBS *pbs, U32 n)
{
    printf("test_run : n=%u\n",n);

if (0) 
{
    // trace_run

    for (U32 i=0;i<n;i++)
    {
        TBS* p=pbs+i;
        printf("  ord : %s %4u\n", ((p->typ & BUY)?"B":"S"), p->qty);        
    }
    printf("running : \n");
}

    TSLOT slot;
    slot.fr = chunk_get();
    slot.bk = slot.fr;

    // process orders 

    PHEAD trs=NULL;

    for (U32 i=0;i<n;i++)
    {
        TBS* p=pbs+i;
        printf("  ord : %s %4u\n", ((p->typ & BUY)?"B":"S"), p->qty);        

        trs = scan_match_order(&slot,  p->typ,  p->qty, PRIC);

        //trace_chunk(slot.fr);

        if (trs)
        {
            printf("  mat\n");
            chunk_rel(trs);
        }
    }
}

void test_cases()
{
    TBS tst01[] = { 
        {BUY,20},       // B20
        {BUY,10},       // B30
    };

    TBS tst02[] = { 
        {BUY,30},       // B20
        {BUY,20},       // B50
        {SEL,20},       // B30
    };

    TBS tst03[] = { 
        {BUY,20},       // B20
        {BUY,10},       // B30
        {SEL,10},       // B20
        {SEL,10},       // B10
        {SEL,10},       //  - 
        {SEL,10},       // S10
    };

    TBS tst04[] = { 
        {BUY,16},       // B16
        {SEL,22},       // S22
    };

    test_run(tst01, _count_of(tst01)); 
    test_run(tst02, _count_of(tst02)); 
    test_run(tst03, _count_of(tst03)); 
    test_run(tst04, _count_of(tst04)); 
}

void test_rand_trades(U32 N)
{
    TSLOT slot;
    slot.fr = chunk_get();
    slot.bk = slot.fr;

    PHEAD trs=NULL;

    for (U32 i=0;i<N;i++)
    {
        U32 qty = 1+randn(10)+randn(10)+randn(10);
        U32 otyp = (randn(2)==1) ? BUY : SEL;

        trs = scan_match_order(&slot,  otyp, qty, PRIC);

        //trace_chunk(slot.fr);

        if (trs)
        {
            // log the trade //TODO
            chunk_rel(trs);
        }
    }

    printf("nallocs : %u\n",nallocs);
}


void test_allocs(void)
{
    int w64=8;
    printf("sizeofs : \n");
    printf("  sizeof   type size_t  : %ld\n",sizeof(size_t));
    printf("\n");
    printf("  sizeof struct THEAD   : %ld b  [ %ld 64s ]\n",sizeof(THEAD), sizeof(THEAD)/w64); 
    printf("\n");
    printf("  sizeof   enum ORD_TYP : %ld\n",sizeof(ORD_TYP));
    printf("  sizeof struct TORDER  : %ld b  [ %ld 64s ]\n",sizeof(TORDER), sizeof(TORDER)/w64);
    printf("\n");
    //exit(-1);


    printf("some chunk_alloc() s\n");

    PHEAD chk1=chunk_alloc();                           // >> 1
    PHEAD chk2=chunk_alloc();                           // >> 2
    PHEAD chk3=chunk_alloc();                           // >> 3

    printf("  chunk_rel chk2\n");
    chunk_rel(chk2);                                    // << 2
    show_chain(freelist);


    PHEAD chk4=chunk_get();                             // >> 4 aka 2
    chunk_rel(chk4);                                    // << 4 aka 2
    printf("  chunk_rel chk1\n");
    chunk_rel(chk1);                                    // << 1
    show_chain(freelist);


    printf("  get 5 chunks .. [ need to alloc 2 more ]\n");
    PHEAD x1=chunk_get();
    PHEAD x2=chunk_get();
    PHEAD x3=chunk_get();
    PHEAD x4=chunk_get();
    PHEAD x5=chunk_get();


    printf("before and after lots of rels\n");

    show_chain(freelist);
    chunk_rel(chk3);                                    // << 3     // TODO //prevent double free : mark as on freelist
    chunk_rel(x3);
    chunk_rel(x1);
    chunk_rel(x2);
    chunk_rel(x5);
    chunk_rel(x4);
    show_chain(freelist);


    printf("\nadding some items ...\n");
  
    PHEAD chk = chunk_get(); 

    PORDER po1 = chunk_new_order(chk,  BUY | LIMIT, 30, PRIC);
    PORDER po2 = chunk_new_order(chk,  BUY,         20, PRIC);
    PORDER po3 = chunk_new_order(chk,  SEL,         25, PRIC);

    show_chain(chk);
    show_chain(freelist);

    printf("few more orders : \n");

    PORDER po4 = chunk_new_order(chk,  BUY,         20, PRIC);
    PORDER po5 = chunk_new_order(chk,  SEL,         25, PRIC);
    PORDER po6 = chunk_new_order(chk,  BUY,         20, PRIC);
    PORDER po7 = chunk_new_order(chk,  SEL,         25, PRIC);
    PORDER po8 = chunk_new_order(chk,  SEL,         25, PRIC);
    show_chain(chk);
    show_chain(freelist);

    printf("  rand orders added : \n");
    for (int i=0;i<31;i++)
        rand_order(chk);

    show_chain(chk);

    show_chain(freelist);
}


///


//const U32 NPRICECHUNK = 1 << 8;          // 8192 = 2 ^ 13
const U32 NPRICECHUNK = ( 1 << 13 ) - 2;          // 8192 = 2 ^ 13


typedef struct TPRICE_LST               // [ pri prv nxt ] [ array of NPRICECHUNK TSLOTs, one per price]
{
    U32                 lopri;          // starting price of slot 0
    U32                 n;
    struct TPRICE_LST * prv;
    struct TPRICE_LST * nxt;
    U64                 spacer;
} TPRICE_LST;
typedef struct TPRICE_LST *PPRICE_LST;


PPRICE_LST pricehead=NULL;



PPRICE_LST price_slots_alloc(U32 lo_price)
{
    size_t sz=0;
    sz += sizeof(TPRICE_LST);
    sz += NPRICECHUNK*sizeof(TSLOT);

    PPRICE_LST p = malloc(sz);
    memset(p,0,sz);
    p->n=NPRICECHUNK;
    p->lopri = NPRICECHUNK*(lo_price/NPRICECHUNK);

    printf("price_slots_alloc : %u\n",p->lopri);
    return p;
}

int price_slot_comp_price(PPRICE_LST ps, U32 pri)        // -1 pri below, 0 pri within, +1 pri above
{
    U32 lo = ps->lopri; 
    U32 hi = lo + NPRICECHUNK;

    if (pri < lo)
        return -1;
    if (hi <= pri)
        return +1;
    return 0;
}

PPRICE_LST price_slots_alloc_between(PPRICE_LST plo, U32 pri, PPRICE_LST phi)
{
    PPRICE_LST pmd = price_slots_alloc(pri);

    if (phi)
        phi->prv = pmd;

    pmd->nxt = phi; 
    pmd->prv = plo;

    if (plo)
        plo->nxt = pmd;

    return pmd;
}

PPRICE_LST price_slots_for(PPRICE_LST ps, U32 pri)
{
    int updn = price_slot_comp_price(ps, pri);

    if (updn==0)
        return ps;

    if (updn<0)
    {
        if (!ps->prv)
            return price_slots_alloc_between(NULL, pri, ps);

        updn = price_slot_comp_price(ps->prv, pri);

        if (updn>0)
            return price_slots_alloc_between(ps->prv, pri, ps);
            
        return price_slots_for(ps->prv, pri); 
    }
    //if (updn>0)
    else
    {
        if (!ps->nxt)
            return price_slots_alloc_between(ps, pri, NULL);

        updn = price_slot_comp_price(ps->nxt, pri);

        if (updn<0)
            return price_slots_alloc_between(ps, pri, ps->nxt);
            
        return price_slots_for(ps->nxt, pri);
    }
}

void test_prices_list()
{
    pricehead = price_slots_alloc(5000);

    PPRICE_LST pshi = price_slots_for(pricehead, 6000);
    assert(price_slot_comp_price(pshi, 6000)==0);

    PPRICE_LST pslo = price_slots_for(pricehead, 4000);
    assert(price_slot_comp_price(pslo, 4000)==0);

    // trace price chunks lo .. hi

    PPRICE_LST p=NULL;

    printf("prices lo to hi\n");
    p=pslo;
    while(p)
    {
        printf("  slots : [%u...%u)\n", p->lopri, p->lopri+NPRICECHUNK); 
        p=p->nxt;
    }

    // trace price chunks hi .. lo

    printf("prices hi to lo\n");
    p=pshi;
    while(p)
    {
        printf("  slots : [%u...%u)\n", p->lopri, p->lopri+NPRICECHUNK); 
        p=p->prv;
    }

    printf("prices from 0 .. 10000\n");

    p=pricehead;
    for (int i=0;i<100000;i++)
    {
        int pri = randn(100000);
        p = price_slots_for(p, pri);
        assert(price_slot_comp_price(p, pri)==0);
    }

    p=pricehead;
    p = price_slots_for(p, 100005);
    printf("prices hi to lo\n");
    while(p)
    {
        printf("  slots : [%u...%u)\n", p->lopri, p->lopri+NPRICECHUNK); 
        p=p->prv;
    }
}

PSLOT prices_slot_get(U32 pri)
{
    if (!pricehead)
        pricehead = price_slots_alloc(pri);

    // get the right prices chunk list, containing this price slot [ or make one ]

    PPRICE_LST ps = price_slots_for(pricehead, pri);
    assert(price_slot_comp_price(ps, pri)==0);

    // get the right slot entry for this price, within the price chunk list

    PSLOT pslot=(PSLOT)(ps+1);
    U32 ks = pri - ps->lopri;
    pslot += ks;                       

    // init an orders chunklist at that price slot, if none there already

    if (!pslot->fr)
        pslot->bk = pslot->fr = chunk_get();

    return pslot;
}


static uint64_t tsns( void )
{
    struct timespec now;
    //clock_gettime( CLOCK_MONOTONIC_RAW, &now );
    clock_gettime( CLOCK_REALTIME, &now );
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

void test_multi_slots()
{
    U32 bpri=32000;
    pricehead = price_slots_alloc(bpri);

    // prealloc some prices we are likely to hit ... to avoid long latency of allocating
if (0) {
    price_slots_for(pricehead,33280);
    price_slots_for(pricehead,33024);
    price_slots_for(pricehead,32768);
    price_slots_for(pricehead,32512);
    price_slots_for(pricehead,32256);
    price_slots_for(pricehead,31744);
    price_slots_for(pricehead,31488);
    price_slots_for(pricehead,31232);
    price_slots_for(pricehead,30976);

    // prealloc some onto the orders freelist 

    for (int j=0;j<7000;j++)
    {
        PHEAD chk=chunk_alloc();
        chunk_rel(chk);
    }
}
    nallocs=0;      // only count those extra ones during run, not preloads

    // run the actual test ..

    U32 ntrade=0;
    U32 norder=0;
    U32 nslow=0;

    printf("ready ..\n");

    U64 tm0=tsns();
    U64 tml=tm0;
    U64 nsmax=0;
    U64 dtfull = 0;
    for (int i=0;i<1000000;i++)
    {
        int bs = (randn(2)==1) ? +1 : -1;

        norder++;

        U32 qty     = 1+randn(10)+randn(10)+randn(10);
        U32 pri     = bpri + ( randn(5)+randn(5) - 4 );
        U32 otyp    = (bs>0) ? BUY : SEL;

        tml=tsns();     // dont count the rand order prep time

        assert(pri>0);
        //printf("  >  %s %3u %6u\n", ((otyp & BUY)?"B":"S"), qty, pri);

        PSLOT pslot=prices_slot_get(pri);       // faster way to get slot ? pslot_buy pslot_sel .. and step up/down from those within prices run

        PHEAD trs = scan_match_order(pslot, otyp, qty, pri);
        if (trs)
        {
            bpri=pri;       // pri recenters around trades

            ntrade++;

            //printf("trade : %u\n",pri);

            chunk_rel(trs);
        }

        U64 nw = tsns();
        U64 dt = nw-tml;
        nsmax=MAX(nsmax,dt);
        tml=nw;
        dtfull+=dt;
        if (dt>2000)
            nslow++;        // slower than 2000ns / 2us

        latency_log(dt);
    }

    //U64 dtfull = tml-tm0;

    printf("tot trades : %12u\n",ntrade);
    printf("tot orders : %12u\n",norder);
    printf("tot allocs : %12u\n",nallocs);
    //printf("tot  nanos : %12lu\n",dtfull);
    printf("max  dt ns : %12lu\n",nsmax);
    //printf("mean    ns : %12.3lf\n",((double)dtfull/norder));
    printf("mean    ns : %12lu\n",(dtfull/norder));
    printf("tot slows  : %12u\n",nslow);

    latency_trace();
}

int main(void)
{
    srand(43532);

    //test_cases();

    //test_rand_trades(1000000);    

    //test_prices_list();

    test_multi_slots();

    return 0;
}

