using MaxMind.GeoIP;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.Caching;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace 网站统计数据生成
{
    public partial class Form1 : Form
    {
        public bool isrun = true;
        public Thread t = null;

        string _house = ConfigurationManager.AppSettings["mongodbconn"].ToString();

        public Form1()
        {
            InitializeComponent();
            System.Windows.Forms.Control.CheckForIllegalCrossThreadCalls = false;
            List<A> list = new List<A>() {
                new A {id=1,name="年" },
                new A {id=2,name="月" },
                new A {id=3,name="周" },
                new A {id=4,name="日" }
            };
            string[] items = new string[] { "年", "月", "周", "日" };
            comboBox1.DisplayMember = "name";
            comboBox1.ValueMember = "id";

            comboBox1.DataSource = list;
            comboBox1.SelectedValue = 3;
            timer1.Interval = 3 * 3600 * 1000;
            timer1.Enabled = true;
        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (button1.Text == "开始统计")
            {
                //timer1.Enabled = false;
                isrun = true;
                button1.Enabled = false;
                button1.Text = "停止";
                listBox1.Items.Clear();
                listBox1.Items.Insert(0, "统计启动，请稍候......" + DateTime.Now.ToString());
                
                t = new Thread(DoIt);
                t.Start();
            }
            else
            {
                timer1.Enabled = true;
                isrun = false;
                button1.Text = "开始统计";
                listBox1.Items.Insert(0, "统计停止." + DateTime.Now.ToString());

                if (t != null)
                {
                    t.Abort();
                    t = null;
                }
            }
        }

        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (t != null)
            {
                t.Abort();
                t = null;
            }
            isrun = false;
            Application.Exit();
        }

        /// <summary>
        /// 统计后入库
        /// </summary>
        /// <returns></returns>
        private void DoIt()
        {
            int time = Convert.ToInt16(comboBox1.SelectedValue);
            DateTime sdate = DateTime.Today.AddDays(-1);
            DateTime edate = DateTime.Today;

            switch (time)
            {
                case 1:
                    sdate = DateTime.Today.AddYears(-1);//一年
                    break;
                case 2:
                    sdate = DateTime.Today.AddMonths(-1);//一月
                    break;
                case 3:
                    sdate = DateTime.Today.AddDays(-7);//一周
                    break;
            }
            var conn = Conn();
            var option = new AggregateOptions();
            option.AllowDiskUse = true;

            //1-影院，2-成人，3-旧版,4-澳洲
            //BsonDocument oldverMatch = new BsonDocument { { "Host", new BsonDocument("$eq", "classic.ifun.tv") } };
            BsonDocument bd_ip = new BsonDocument { { "_id", "$IP" } };
            BsonDocument bd_uuid = new BsonDocument { { "_id", "$UUID" } };
            BsonDocument bd_sessionid = new BsonDocument { { "_id", "$SessionID" } };

            for (int k = 29; k <= 56; k++)
            {
                //if(k>30 && k<51)
                //{
                //    continue;
                //}

                string _host = string.Empty;
                switch (k)
                {
                    //wyav.tv
                    case 1:
                    case 31:
                    case 32:
                        _host = "wyav.tv";
                        listBox1.Items.Insert(0, k + ".正在生成pc午夜版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //m.wyav.tv
                    case 4:
                    case 5:
                    case 33:
                    case 34:
                        _host = "m.wyav.tv";
                        listBox1.Items.Insert(0, k + ".正在生成安卓移动午夜版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //iyf.tv
                    case 12:
                    case 24:
                    case 35:
                    case 36:
                        _host = "iyf.tv";
                        listBox1.Items.Insert(0, k + ".正在生成pc影院版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //m.iyf.tv
                    case 25:
                    case 26:                   
                    case 37:
                    case 38:
                        _host = "m.iyf.tv";
                        listBox1.Items.Insert(0, k + ".正在生成移动影院版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //akid.tv
                    case 16:
                    case 47:
                    case 48:
                        _host = "akid.tv";
                        listBox1.Items.Insert(0, k + ".正在生成儿童桌面版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //m.akid.tv
                    case 17:                   
                    case 18:
                    case 49:
                    case 50:
                        _host = "m.akid.tv";
                        listBox1.Items.Insert(0, k + ".正在生成儿童安卓版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //flyv.tv
                    case 20:
                    case 39:
                    case 40:
                        _host = "flyv.tv";
                        listBox1.Items.Insert(0, k + ".正在生成flyv桌面版日志数据，请稍候..." + DateTime.Now.ToString()); 
                        break;

                    //m.flyv.tv
                    case 21:                   
                    case 22:
                    case 41:
                    case 42:
                        _host = "m.flyv.tv";
                        listBox1.Items.Insert(0, k + ".正在生成flyv安卓版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //tv
                    case 27:
                    case 56:
                        _host = "classic.iyf.tv";
                        listBox1.Items.Insert(0, k + ".正在生成电视浏览版日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //app
                    case 29:
                    case 30:
                    case 51:
                    case 52:
                    case 53:
                    case 54:
                    case 55:
                        _host = "app.apiregion721.xyz";
                        listBox1.Items.Insert(0, k + ".正在生成桌面App日志数据，请稍候..." + DateTime.Now.ToString());
                        break;

                    //new.iyf.tv
                    case 45:
                    case 46:
                        _host = "new.iyf.tv";
                        listBox1.Items.Insert(0, k + ".正在生成new.iyf日志数据，请稍候..." + DateTime.Now.ToString());
                        break;
                    default:
                        continue;                        
                }

                BsonDocument dbMatch = new BsonDocument { { "CID", new BsonDocument("$eq", k) } };
                //BsonDocument domainMatch = new BsonDocument { { "Host", new BsonDocument("$eq", _host) }, { "CID", new BsonDocument("$eq", k) } };

                //1.统计ip,uv,pv值
                for (DateTime i = sdate; i < edate; i = i.AddDays(1))
                {
                    var y_collection = conn.GetCollection<SiteCount>("log_" + i.ToString("yyyyMMdd"));
                    int _year = i.Year;
                    int _month = i.Month;
                    int _day = i.Day;

                    try
                    {
                        //1.统计ippvuv:先看是否存在，如果不存在则统计;如果存在则不统计了
                        string tablename = "tj_ippvuv";
                        int _ip = 0;
                        int _uv = 0;
                        int _pv = 0;

                        var a_collection = conn.GetCollection<B>(tablename);
                        if (a_collection != null)
                        {
                            var query = from a in a_collection.AsQueryable()
                                        where a.Year == _year && a.Month == _month && a.Day == _day && a.CID == k
                                        select a.Id;
                            if (!query.Any())
                            {
                                var cidinfos = y_collection.Aggregate(option).Match(dbMatch);
                                var aggregate_ip = cidinfos.Group(bd_ip);
                                _ip = aggregate_ip.ToList().Count;
                                var aggregate_sessionid = cidinfos.Group(bd_sessionid);
                                _uv = aggregate_sessionid.ToList().Count;
                                _pv = y_collection.AsQueryable(option).Where(p => p.CID == k).Sum(p => p.Count);

                                if (_ip > 0 && _pv > 0 && _uv > 0)
                                {
                                    a_collection.InsertOneAsync(new B
                                    {
                                        Year = _year,
                                        Month = _month,
                                        Day = _day,
                                        IP = _ip,
                                        PV = _pv,
                                        UV = _uv,
                                        CID = k
                                    });
                                    listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "ip,pv,uv统计");
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "提示：" + _year + "-" + _month + "-" + _day + "下无数据");
                                }
                            }
                            else
                            {
                                listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "ip,pv,uv统计");
                            }
                        }

                        //如果是app则有如下统计
                        if (k == 29 || k == 30 || (k>=51 && k<=55))
                        {
                            //统计uuid的情况
                            tablename = "tj_uuid";
                            var u_collection = conn.GetCollection<H>(tablename);
                            if (u_collection != null)
                            {
                                var query = from a in u_collection.AsQueryable()
                                            where a.Year == _year && a.Month == _month && a.Day == _day && a.CID == k
                                            select a.Id;
                                if (!query.Any())
                                {
                                    BsonDocument appMatch = new BsonDocument { { "CID", new BsonDocument("$eq", k) } };
                                    var cidinfos = y_collection.Aggregate(option).Match(appMatch);
                                    var aggregate_uuid = cidinfos.Group(bd_uuid);
                                    int _uuid = aggregate_uuid.ToList().Count;

                                    appMatch = new BsonDocument { { "TodayVisit", new BsonDocument("$eq", 1) }, { "CID", new BsonDocument("$eq", k) } };
                                    var todayinfos = y_collection.Aggregate(option).Match(appMatch);
                                    var aggregate_active = todayinfos.Group(bd_uuid);
                                    int _activeuid = aggregate_active.ToList().Count;
                                    if (_uuid > 0)
                                    {
                                        u_collection.InsertOneAsync(new H
                                        {
                                            Year = _year,
                                            Month = _month,
                                            Day = _day,
                                            UUID = _uuid,
                                            TodayActive = _activeuid,
                                            CID = k,
                                            After1 = 0,
                                            After2 = 0,
                                            After3 = 0,
                                            After4 = 0,
                                            After5 = 0,
                                            After6 = 0,
                                            After7 = 0,
                                            After14 = 0,
                                            After30 = 0
                                        });
                                        listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "uuid,activeuid统计");
                                    }
                                    else
                                    {
                                        listBox1.Items.Insert(0, "提示：" + _year + "-" + _month + "-" + _day + "下无数据");
                                    }
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "uuid,activeuid统计");
                                }
                            }

                            //统计版本情况 iyf\/.+?\s
                            Dictionary<string, int> dict = new Dictionary<string, int>();
                            tablename = "tj_ver";
                            var ver_collection = conn.GetCollection<Ver>(tablename);
                            if (ver_collection != null)
                            {
                                var query = from a in ver_collection.AsQueryable()
                                            where a.Year == _year && a.Month == _month && a.Day == _day && a.CID == k
                                            select a.Id;
                                if (!query.Any())
                                {
                                    var cidInfosList = (from a in y_collection.AsQueryable()
                                                        where a.CID == k && a.system!="" && a.appVersion!=""
                                                        select new
                                                        {
                                                            a.Package,
                                                            a.UUID,
                                                            a.appVersion,
                                                            a.system
                                                        }).ToList().GroupBy(p => p.Package).Select(p => new
                                                        {
                                                            package = p.Key,
                                                            others=p
                                                        }).ToList();

                                    foreach (var item in cidInfosList)
                                    {
                                        string _package = item.package;
                                        var temps= item.others.GroupBy(p => p.appVersion).Select(p => new {
                                            version=p.Key,
                                            num=p.Select(w=>w.UUID).Distinct().Count()
                                        }).OrderBy(p=>p.version);
                                        foreach(var temp in temps)
                                        {
                                            ver_collection.InsertOneAsync(new Ver
                                            {
                                                Year = _year,
                                                Month = _month,
                                                Day = _day,
                                                CID = k,
                                                Version = temp.version,
                                                Num = temp.num,
                                                PackageName=_package
                                            });
                                        }
                                    }

                                    listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "ver统计");
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "ver统计");
                                }
                            }
                        }
                    }
                    catch(Exception ex)
                    {
                        listBox1.Items.Insert(0, _year + "-" + _month + "-" + _day + ex.Message);
                        GC.Collect();
                        Thread.Sleep(12000);
                    }
                }

                //2.按域名进行统计
                for (DateTime i = sdate; i < edate; i = i.AddDays(1))
                {
                    var y_collection = conn.GetCollection<SiteCount>("log_" + i.ToString("yyyyMMdd"));
                    int _year = i.Year;
                    int _month = i.Month;
                    int _day = i.Day;
                    
                    try
                    {
                        //2.统计按设备(桌面,移动,app,tv)
                        string tablename = "tj_set";
                        var b_collection = conn.GetCollection<C>(tablename);
                        if (b_collection != null)
                        {
                            var query = from b in b_collection.AsQueryable()
                                        where b.Year == _year && b.Month == _month && b.Day == _day && b.CID == k
                                        select b.Id;
                            if (!query.Any())
                            {
                                //按域名                              
                                var hostinfos = y_collection.Aggregate(option).Match(dbMatch);
                                int host_ip = hostinfos.Group(bd_ip).ToList().Count();
                                if (host_ip > 0)
                                {
                                    b_collection.InsertOneAsync(new C
                                    {
                                        Year = _year,
                                        Month = _month,
                                        Day = _day,
                                        Num = host_ip,
                                        SetName = _host,
                                        CID = k
                                    });

                                    listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "域名统计");
                                    //GC.Collect();
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "提示：" + _year + "-" + _month + "-" + _day + "下无数据");
                                }
                            }
                            else
                            {
                                listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "域名统计");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        listBox1.Items.Insert(0, _year + "-" + _month + "-" + _day + "域名的统计异常！");
                        GC.Collect();
                        Thread.Sleep(12000);
                    }
                }

                //3.按使用操作系统进行统计
                for (DateTime i = sdate; i < edate; i = i.AddDays(1))
                {
                    var y_collection = conn.GetCollection<SiteCount>("log_" + i.ToString("yyyyMMdd"));
                    int _year = i.Year;
                    int _month = i.Month;
                    int _day = i.Day;
                    
                    try
                    {
                        string tablename = "tj_os";
                        var c_collection = conn.GetCollection<D>(tablename);
                        if (c_collection != null)
                        {
                            var query = from c in c_collection.AsQueryable()
                                        where c.Year == _year && c.Month == _month && c.Day == _day && c.CID == k
                                        select c.Id;
                            if (!query.Any())
                            {
                                BsonDocument bd = new BsonDocument { { "X", "$UserAgent" }, { "Y", "$IP" } };                              
                                List<D> list = new List<D>();
                                if (k == 29 || k == 30 || (k >= 51 && k <= 55))
                                {
                                    var infos = (from a in y_collection.AsQueryable()
                                                 where a.CID == k
                                                 select new
                                                 {
                                                     os = a.system ?? "",
                                                     uuid = a.UUID
                                                 }).ToList();
                                    var temps = infos.Where(p=>p.os!="").GroupBy(p => p.os).ToList();
                                    foreach (var info in temps)
                                    {
                                        var _osname = info.Key;
                                        var _ipnum = info.GroupBy(p => p.uuid).Count();
                                        if (_ipnum > 0)
                                        {
                                            D d = new D();
                                            d.Year = _year;
                                            d.Month = _month;
                                            d.Day = _day;
                                            d.Num = _ipnum;
                                            d.OSName = _osname;
                                            d.CID = k;
                                            list.Add(d);
                                        }
                                    }
                                }
                                else
                                {
                                    var aggregate = y_collection.Aggregate(option).Match(dbMatch).Project(bd);
                                    var infos = aggregate.ToEnumerable().Select(p => new
                                    {
                                        os = GetCache(p.Values.Skip(1).First().ToString()),
                                        ip = p.Values.Skip(2).First(),
                                    }).GroupBy(p => p.os);
                                    foreach (var info in infos)
                                    {
                                        var _osname = info.Key;
                                        var _ipnum = info.GroupBy(p => p.ip).Count();
                                        if (_ipnum > 0)
                                        {
                                            D d = new D();
                                            d.Year = _year;
                                            d.Month = _month;
                                            d.Day = _day;
                                            d.Num = _ipnum;
                                            d.OSName = _osname;
                                            d.CID = k;
                                            list.Add(d);
                                        }
                                    }
                                }                              

                                if (list.Count > 0)
                                {
                                    c_collection.InsertManyAsync(list);
                                    listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "操作系统统计");
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "提示：" + _year + "-" + _month + "-" + _day + "下无数据");
                                }
                            }
                            else
                            {
                                listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "操作系统统计");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        listBox1.Items.Insert(0, ex.Message);
                        GC.Collect();
                        Thread.Sleep(12000);
                    }
                }

                //4.统计按国家
                for (DateTime i = sdate; i < edate; i = i.AddDays(1))
                {
                    var y_collection = conn.GetCollection<SiteCount>("log_" + i.ToString("yyyyMMdd"));
                    int _year = i.Year;
                    int _month = i.Month;
                    int _day = i.Day;
                    
                    try
                    {
                        string tablename = "tj_country";
                        var d_collection = conn.GetCollection<E>(tablename);
                        if (d_collection != null)
                        {
                            var query = from d in d_collection.AsQueryable()
                                        where d.Year == _year && d.Month == _month && d.Day == _day && d.CID == k
                                        select d.Id;
                            if (!query.Any())
                            {
                                List<E> list = new List<E>();
                                var infos = y_collection.AsQueryable(option).Where(p => p.CID == k).GroupBy(p => p.IP).Select(p=>new {
                                   Key= p.Key,
                                   Count= p.Count()
                                }).ToList();
                                var aaa=infos.Select(p => new
                                {
                                    Country = GetCountryNameByIP(p.Key),
                                    Num = p.Count,
                                }).ToList();
                                var bbb=aaa.GroupBy(p => p.Country);

                                foreach (var info in bbb)
                                {
                                    list.Add(new E
                                    {
                                        Country = info.Key,
                                        Num = info.Sum(p => p.Num),
                                        Day = _day,
                                        Month = _month,
                                        Year = _year,
                                        CID = k
                                    });
                                }

                                if (list.Count > 0)
                                {
                                    d_collection.InsertManyAsync(list.OrderByDescending(p => p.Num).Take(15));
                                    listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "国家统计");
                                    //GC.Collect();
                                    //Thread.Sleep(10000);
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "提示：" + _year + "-" + _month + "-" + _day + "下无数据");
                                }
                            }
                            else
                            {
                                listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "国家统计");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        listBox1.Items.Insert(0, _year + "-" + _month + "-" + _day + "国家的统计异常！");
                        GC.Collect();
                        Thread.Sleep(12000);
                    }
                }

                //skin
                for (DateTime i = sdate; i < edate; i = i.AddDays(1))
                {
                    var y_collection = conn.GetCollection<SiteCount>("log_" + i.ToString("yyyyMMdd"));
                    int _year = i.Year;
                    int _month = i.Month;
                    int _day = i.Day;

                    try
                    {
                        string tablename = "tj_skin";
                        var e_collection = conn.GetCollection<G>(tablename);
                        if (e_collection != null)
                        {
                            var query = from d in e_collection.AsQueryable()
                                        where d.Year == _year && d.Month == _month && d.Day == _day && d.CID == k
                                        select d.Id;
                            if (!query.Any())
                            {
                                List<G> list = new List<G>();
                                var infos = y_collection.AsQueryable(option).Where(p => p.CID == k).GroupBy(p => p.Skin).Select(p => new {
                                    Key = p.Key,
                                    Count = p.Sum(w=>w.Count)//pv
                                }).ToList();

                                foreach (var info in infos)
                                {
                                    list.Add(new G
                                    {
                                        Skin = info.Key,
                                        Num = info.Count,
                                        Day = _day,
                                        Month = _month,
                                        Year = _year,
                                        CID = k
                                    });
                                }

                                if (list.Count > 0)
                                {
                                    e_collection.InsertManyAsync(list);
                                    listBox1.Items.Insert(0, "完成" + _year + "-" + _month + "-" + _day + "skin统计");
                                    //GC.Collect();
                                    //Thread.Sleep(10000);
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "提示：" + _year + "-" + _month + "-" + _day + "下无数据");
                                }
                            }
                            else
                            {
                                listBox1.Items.Insert(0, "跳过" + _year + "-" + _month + "-" + _day + "skin统计");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        listBox1.Items.Insert(0, _year + "-" + _month + "-" + _day + "skin的统计异常！");
                        GC.Collect();
                        Thread.Sleep(12000);
                    }
                }
            }

            listBox1.Items.Insert(0, "开始进行留存更新" + DateTime.Now.ToString());

            //计算日活用户的留存率
            for (int cid = 29; cid <= 55; cid++)
            {
                if(cid>30 && cid<51)
                {
                    continue;
                }

                for (DateTime i = sdate.AddDays(-7); i < edate; i = i.AddDays(1))
                {
                    listBox1.Items.Insert(0, "开始统计" + i.ToShortDateString() + "日的留存更新");

                    //每天都要计算出它的后7天的uuid还有多少？
                    //var y_collection = conn.GetCollection<SiteCount>("log_" + i.ToString("yyyyMMdd"));
                    int _year = i.Year;
                    int _month = i.Month;
                    int _day = i.Day;
                    try
                    {
                        string tablename = "tj_uuid";
                        var e_collection = conn.GetCollection<H>(tablename);
                        if (e_collection != null)
                        {
                            var query = (from d in e_collection.AsQueryable()
                                         where d.Year == _year && d.Month == _month && d.Day == _day && d.CID == cid
                                         select d ).FirstOrDefault();
                            if (query != null)
                            {
                                bool ret = UpdateAfterDayNum(query);
                                if (ret)
                                {
                                    listBox1.Items.Insert(0, "已完成" + i.ToShortDateString() + "日的留存更新");
                                }
                                else
                                {
                                    listBox1.Items.Insert(0, "提示：更新" + i.ToShortDateString() + "日的留存时异常了");
                                }
                            }
                            else
                            {
                                listBox1.Items.Insert(0, "没有找到相应的数据" + DateTime.Now.ToString());
                            }
                        }
                    }
                    catch(Exception ex)
                    {
                        listBox1.Items.Insert(0, "统计异常" + ex.Message + DateTime.Now.ToString());
                    }

                }
            }

            //isrun = false;
            button1.Enabled = true;
            button1.Text = "开始统计";
            listBox1.Items.Insert(0, "统计完成." + DateTime.Now.ToString());
            GC.Collect();

            timer1.Enabled = true;

            if (t != null)
            {
                t.Abort();
                t = null;
            }
        }

        /// <summary>
        /// 更新某条记录的UUID它的后一个星期的留存
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private bool UpdateAfterDayNum(H model)
        {
            bool result = true;
            if (model != null)
            {
                DateTime d = new DateTime(model.Year, model.Month, model.Day);               

                for (int i = 1; i <= 30; i++)
                {
                    if((i>7 && i<14) || (i>14 && i<30))
                    {
                        continue;
                    }

                    double ret = 0;
                    DateTime otherDay = d.AddDays(i);//i天后
                    if (otherDay < DateTime.Today)
                    {
                        ret = GetRate(d, otherDay,model.CID);
                        switch (i)
                        {
                            case 1: model.After1 = ret; break;
                            case 2: model.After2 = ret; break;
                            case 3: model.After3 = ret; break;
                            case 4: model.After4 = ret; break;
                            case 5: model.After5 = ret; break;
                            case 6: model.After6 = ret; break;
                            case 7: model.After7 = ret; break;
                            case 14: model.After14 = ret; break;
                            case 30: model.After30 = ret; break;
                        }
                    }
                }

                //都是0就不用更新
                if (model.After1 == 0 && model.After2 == 0 && model.After3 == 0 && model.After4 == 0 && model.After5 == 0 && model.After6 == 0 && model.After7 == 0 && model.After14 == 0 && model.After30 == 0)
                {
                    return true;
                }
                //更新数据
                return UpdateData(model);
            }
            return result;
        }

        /// <summary>
        /// 计算留存(精细到某类设备)
        /// </summary>
        /// <param name="baseDay"></param>
        /// <param name="otherDay"></param>
        /// <returns></returns>
        private double GetRate(DateTime baseDay, DateTime otherDay,int cid)
        {
            double result = 0;
            List<string> orgials = GetUUIDList(baseDay,true,cid);
            List<string> currents = GetUUIDList(otherDay,false,cid);
            var tmplist = orgials.Intersect(currents);
            if (orgials.Count > 0)
            {
                result = Math.Round((double)tmplist.Count() / orgials.Count, 4);
            }
            return result;
        }

        /// <summary>
        /// 获取某天的UUID列表
        /// </summary>
        /// <param name="day"></param>
        /// <returns></returns>
        private List<string> GetUUIDList(DateTime day,bool isNewsAdd,int cid)
        {
            List<string> result = new List<string>();
            var conn = Conn();
            BsonDocument bd_uuid = new BsonDocument { { "_id", "$UUID" } };
            var y_collection = conn.GetCollection<SiteCount>("log_" + day.ToString("yyyyMMdd"));
            var option = new AggregateOptions();
            option.AllowDiskUse = true;
            int _todayVisit = isNewsAdd ? 1 : 0;
            result = y_collection.AsQueryable(option).Where(p=>p.TodayVisit== _todayVisit && p.CID==cid).Select(p => p.UUID).Distinct().ToList();
            return result;
        }


        /// <summary>
        /// 将数据写入数据库
        /// </summary>
        /// <param name="model"></param>
        /// <returns></returns>
        private bool UpdateData(H model)
        {
            string dateStr = "cid=" + model.CID + ":" + model.Year + "-" + model.Month + "-" + model.Day;
            try
            {
                var conn = Conn();
                string tablename = "tj_uuid";
                var collection= conn.GetCollection<H>(tablename);
                FilterDefinitionBuilder<H> filterBuilder =new FilterDefinitionBuilder<H>();
                FilterDefinition<H> filter = filterBuilder.And(
                    filterBuilder.Eq("Year", model.Year),
                    filterBuilder.Eq("Month", model.Month),
                    filterBuilder.Eq("Day", model.Day),
                    filterBuilder.Eq("CID", model.CID)
                    );
                var updateDefination = new List<UpdateDefinition<H>>();                
                updateDefination.Add(Builders<H>.Update.Set("After1", model.After1));
                updateDefination.Add(Builders<H>.Update.Set("After2", model.After2));
                updateDefination.Add(Builders<H>.Update.Set("After3", model.After3));
                updateDefination.Add(Builders<H>.Update.Set("After4", model.After4));
                updateDefination.Add(Builders<H>.Update.Set("After5", model.After5));
                updateDefination.Add(Builders<H>.Update.Set("After6", model.After6));
                updateDefination.Add(Builders<H>.Update.Set("After7", model.After7));
                updateDefination.Add(Builders<H>.Update.Set("After14", model.After14));
                updateDefination.Add(Builders<H>.Update.Set("After30", model.After30));
                var combinedUpdate = Builders<H>.Update.Combine(updateDefination);
                collection.UpdateOneAsync(filter, combinedUpdate);
                return true;
            }
            catch
            {
                listBox1.Items.Insert(0, "更新" + dateStr + "日的留存时失败。" + DateTime.Now.ToString());
                return false;
            }
        }


        private string  GetCache(string cacheKey)
        {
            var objCache =(string)MemoryCache.Default[cacheKey];
            if (objCache == null)
            {
                var objObject = AnalysisAgent(cacheKey);
                MemoryCache.Default.Add(cacheKey,objObject,null);
                return objObject;
            }
            return objCache;
        }

        private bool IsIPv6(string addr)
        {
            IPAddress ip;
            if (IPAddress.TryParse(addr, out ip))
            {
                return ip.AddressFamily == AddressFamily.InterNetworkV6;
            }
            else
            {
                return false;
            }
        }

        private string GetCountryNameByIP(string ip)
        {
            string ret = string.Empty;
            string dbfile = string.Empty;
            LookupService cl = null;

            try
            {
                if (IsIPv6(ip))
                {
                    dbfile = Application.StartupPath + "\\GeoIPv6.dat";
                    cl = new LookupService(dbfile, LookupService.GEOIP_MEMORY_CACHE);
                    ret = cl.getCountryV6(ip).getName();
                }
                else
                {
                    dbfile = Application.StartupPath + "\\GeoIP.dat";
                    cl = new LookupService(dbfile, LookupService.GEOIP_MEMORY_CACHE);
                    ret = cl.getCountry(ip).getName();
                    //cl.getLocation(ip).city
                }
            }
            catch { }
            return ret;
        }

        private string AnalysisAgent(string sUsrAg)
        {
            //Hashtable ret = new Hashtable();
            string reuslt = "Other";
            if (!string.IsNullOrEmpty(sUsrAg))
            {
                if (sUsrAg.IndexOf("Macintosh") > -1)
                {
                    //ret.Add("Os_name", "Mac");
                    reuslt = "Mac";
                }
                else if (sUsrAg.IndexOf("Windows NT 6.1") > -1)
                {
                    //ret.Add("Os_name", "windows7");
                    reuslt = "windows7";
                }
                else if (sUsrAg.IndexOf("Windows NT 6.4") > -1 || sUsrAg.IndexOf("Windows NT 10.0") > -1)
                {
                    //ret.Add("Os_name", "windows10");
                    reuslt = "windows10";
                }
                else if (sUsrAg.IndexOf("iPhone") > -1)
                {
                    //ret.Add("Os_name", "IOS(iPhone)");
                    reuslt = "IOS(iPhone)";
                }
                else if (sUsrAg.IndexOf("iPad") > -1)
                {
                    //ret.Add("Os_name", "IOS(iPad)");
                    reuslt = "IOS(iPad)";
                }
                else if (sUsrAg.IndexOf("Android") > -1 || sUsrAg.IndexOf("Linux") > -1)
                {
                    //ret.Add("Os_name", "Android");
                    reuslt = "Android";
                }
            }
            return reuslt;
        }

        public MongoClient client = null;

        private IMongoDatabase Conn()
        {
            try
            {
                string conn = ConfigurationManager.AppSettings["mongodbconn"];
                client = new MongoClient(conn);                
                var database = client.GetDatabase("dnvod");               
                return database;
            }
            catch {
                return null;
            }
        }

        private void DisConn()
        {
            if (client != null)
            {
                client = null;
            }
        }

        /// <summary>
        /// 统计某天日期的IP
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button2_Click(object sender, EventArgs e)
        {
            DateTime _date = DateTime.Today.AddDays(-1);
            try
            {
                _date = Convert.ToDateTime(textBox1.Text.Trim());
            }
            catch { }

            var conn = Conn();
            BsonDocument bd_ip = new BsonDocument { { "_id", "$IP" } };
            var y_collection = conn.GetCollection<SiteCount>("log_" + _date.ToString("yyyyMMdd"));
            var option = new AggregateOptions();
            option.AllowDiskUse = true;
            var cidinfos = y_collection.Aggregate(option);
            var aggregate_ip = cidinfos.Group(bd_ip);
            int _ip = aggregate_ip.ToList().Count;
            label3.Text = _ip + "";
        }

        private void timer1_Tick(object sender, EventArgs e)
        {
            //if (button1.Text == "开始统计")
            //{
                comboBox1.SelectedValue = 4;
                isrun = true;
                button1.Text = "停止";
                listBox1.Items.Clear();
                listBox1.Items.Insert(0, "统计启动，请稍候......" + DateTime.Now.ToString());

                t = new Thread(DoIt);
                t.Start();
            //}
        }
    }

    public class A
    {
        public int id { get; set; }
        public string name { get; set; }
    }

    public class B
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public double IP { get; set; }
        public double PV { get; set; }
        public double UV { get; set; }
        public int CID { get; set; }
    }

    public class C
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public string SetName { get; set; }
        public double Num { get; set; }
        public int CID { get; set; }
    }

    public class D
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public string OSName { get; set; }
        public double Num { get; set; }
        public int CID { get; set; }
    }

    public class E
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public string Country { get; set; }
        public double Num { get; set; }
        public int CID { get; set; }
    }

    public class SiteCount
    {
        public ObjectId _id { get; set; }
        public string SessionID { get; set; }
        public string Host { get; set; }
        public int Count { get; set; }
        public DateTime CreateTime { get; set; }
        public string IP { get; set; }
        public string UserAgent { get; set; }
        public int CID { get; set; }
        public int Skin { get; set; }

        public int TodayVisit { get; set; }

        public string UUID { get; set; } = string.Empty;
        public string Package { get; set; } = string.Empty;
        public string appVersion { get; set; } = string.Empty;
        public string system { get; set; } = string.Empty;
    }

    public class F
    {
        public string OS { get; set; }
        public string Sessionid { get; set; }
    }

    public class G
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int Skin { get; set; }
        public double Num { get; set; }
        public int CID { get; set; }
    }

    public class H
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public double UUID { get; set; }
        public double TodayActive { get; set; }
        public int CID { get; set; }
        public double After1 { get; set; }
        public double After2 { get; set; }
        public double After3 { get; set; }
        public double After4 { get; set; }
        public double After5 { get; set; }
        public double After6 { get; set; }
        public double After7 { get; set; }
        public double After14 { get; set; }
        public double After30 { get; set; }
    }

    public class Ver
    {
        public ObjectId Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int CID { get; set; }
        public string Version { get; set; }
        public int Num { get; set; }

        public string PackageName { get; set; }
    }
}
