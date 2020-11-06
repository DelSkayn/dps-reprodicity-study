#!/usr/bin/env run-cargo-script
//!
//! ```cargo
//! [dependencies]
//! duct = "0.13.4"
//! structopt = "0.3"
//! anyhow = "1.0"
//! spinners = "1.2"
//! ```
extern crate duct;
extern crate structopt;
extern crate anyhow;
extern crate spinners;


use duct::cmd;
use std::{env, path::Path, fs::{File,OpenOptions}, io::Write, collections::HashSet};
use anyhow::{Context,Result,ensure};
use spinners::{Spinner, Spinners};
use structopt::StructOpt;

const XML_HEADER: &'static str =
"<?xml version=\"1.0\"?>
<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>";

const XML_FOOTER: &'static str = "</configuration>";


const HDFS_SITE_BODY: &'static str = 
"<property>
<name>dfs.namenode.name.dir</name> 
<value>/local/ddps2015/giraph/name</value> 
</property>

<property>
<name>dfs.namenode.data.dir</name> 
<value>/local/ddps2015/giraph/data</value> 
</property>

<property>
<name>dfs.datanode.dns.interface</name> 
<value>ib0</value> 
</property>

<property>
<name>dfs.datanode.use.datanode.hostname</name> 
<value>true</value> 
</property>

<property>
<name>dfs.client.use.datanode.hostname</name>
<value>true</value>
</property>

<property>
<name>dfs.replication</name> 
<value>1</value> 
</property>";

fn get_allocated_nodes() -> Result<Vec<String>>{
    let mut set = HashSet::new();
    let output = cmd!("preserve","-llist").stdout_capture().run()?;
    let text_output = String::from_utf8(output.stdout)?;
    for l in text_output.lines(){
        let mut ws = l.split_whitespace();
        ws.next();
        if Some("ddps2015") != ws.next(){
            continue;
        }
        //start
        ws.next();
        ws.next();
       //stop
        ws.next();
        ws.next();
        //state
        ws.next();
        //nhosts
        ws.next();
        while let Some(x) = ws.next(){
            if x == "-"{
                return Ok(Vec::new())
            }
            set.insert(x.to_string());
        }
    }
    Ok(set.drain().collect())
}

fn main() -> Result<()>{
    println!("Setting up environment");
    let home = env::var("HOME").context("getting HOME variable")?;
    //let ps1 = env::var("PS1").context("getting ps1 variable")?;
    //println!("ps1 {}",ps1);
    let working_dir = Path::new(&home).join("giraph");



    if !Path::new("hadoop").exists(){
        println!("no install found unpacking");
        let sp = Spinner::new(Spinners::Line, "unpacking tar".to_string());
        cmd!("mkdir","-p","/tmp/giraph").run().context("creating /tmp/giraph")?;
        cmd!("tar","-xzf","hadoop-1.2.1.tar.gz","-C","/tmp/giraph/").run().context("unpacking tar")?;
        cmd!("mv","/tmp/giraph/hadoop-1.2.1","hadoop").run().context("changing hadoop directory name")?;
        cmd!("rm","-r","/tmp/giraph").run().context("removing /tmp/giraph")?;
        sp.stop();
        println!();


        // Writing hadoop-env.sh file
        {
            let mut hadoop_env = OpenOptions::new().write(true).append(true).open("hadoop/conf/hadoop-env.sh").context("opening hadoop-env.sh")?;
            let java_home = env::var("JAVA_HOME")?;
            writeln!(hadoop_env,"export JAVA_HOME=\"{}\"",java_home)?;
            writeln!(hadoop_env,"export HADOOP_OPTS=\"-Djava.net.preferIPV4Stack=true\"")?;
        }
    }

    let preserved_nodes = get_allocated_nodes()?;
    ensure!(preserved_nodes.len() != 0,"no nodes preserved!");
    let preserved_nodes: Vec<_> = preserved_nodes.into_iter().map(|mut e| {
        println!("FOUND NODE: {}",e);
        let num = e[4..].parse::<u64>().unwrap();
        let res = format!("10.149.0.{}",num);
        println!("IP: {}",res);
        res
    }).collect();

    env::set_current_dir(working_dir).context("setting working directory")?;


    let master_node = preserved_nodes[0].clone();

    
    // Writing hdfs-site.xml file
    {
        let mut hdfs_file = File::create("hadoop/conf/hdfs-site.xml").context("opening hdfs-site.xml")?;
        writeln!(hdfs_file,"{}",XML_HEADER)?;
        writeln!(hdfs_file,"{}",HDFS_SITE_BODY)?;
        writeln!(hdfs_file,"{}",XML_FOOTER)?;
    }

    // Writing mapred-site.xml file
    {
        let mut hdfs_file = File::create("hadoop/conf/mapred-site.xml").context("opening hdfs-site.xml")?;
        writeln!(hdfs_file,"{}",XML_HEADER)?;
        writeln!(hdfs_file,"<property>
        <name>mapred.job.tracker</name>
        <value>{}:9001</value>
        </property>
        <property>
        <name>mapred.acls.enabled</name>
        <value>false</value>
        </property>
        <property>
        <name>mapred.tasktracker.map.tasks.maximum</name>
        <value>8</value>
        </property>
        <property>
        <name>mapred.tasktracker.reduce.tasks.maximum</name>
        <value>8</value>
        </property>
        <property>
        <name>mapred.map.tasks</name>
        <value>8</value>
        </property>
        <property>
        <name>mapred.map.child.java.opts</name>
        <value>-Xmx12000M</value>
        </property>
        <property>
        <name>mapred.reduce.child.java.opts</name>
        <value>-Xmx12000M</value>
        </property>
        <property>
        <name>mapred.tasktracker.dns.interface</name>
        <value>ib0</value>
        </property>
        ",master_node)?;
        writeln!(hdfs_file,"{}",XML_FOOTER)?;
    }

    // Writing core-site.xml file
    {
        let mut core_file = File::create("hadoop/conf/core-site.xml").context("opening core-site.xml")?;
        writeln!(core_file,"{}",XML_HEADER)?;
        writeln!(core_file,"<property> 
        <name>fs.default.name</name> 
        <value>hdfs://{}:9000</value> 
        </property>
        <property>
        <name>hadoop.tmp.dir</name>
        <value>/tmp/ddps2015</value>
        </property>
        ",&master_node)?;
        writeln!(core_file,"{}",XML_FOOTER)?;
    }

    {
        let mut slaves_file = File::create("hadoop/conf/masters").context("opening slaves file")?;
        writeln!(slaves_file,"{}",preserved_nodes[0])?;
        let mut slaves_file = File::create("hadoop/conf/slaves").context("opening slaves file")?;
        for i in preserved_nodes[1..].iter(){
            writeln!(slaves_file,"{}",i)?;
        }
    }

    for i in preserved_nodes.iter(){
        cmd!("ssh",i,"mkdir -p /tmp/ddps2015/").run()?;
        cmd!("ssh",i,"mkdir -p /local/ddps2015/giraph/name").run()?;
        cmd!("ssh",i,"mkdir -p /local/ddps2015/giraph/data").run()?;
    }

    cmd!("ssh",&master_node,"hadoop namenode -format").run()?;
    cmd!("ssh",&master_node,"start-dfs.sh").run()?;
    cmd!("ssh",&master_node,"start-mapred.sh").run()?;

    let cur_dir = std::env::current_dir()?;
    let hadoop_home = cur_dir.join("hadoop");
    env::set_var("HADOOP_HOME",hadoop_home);
    env::set_var("PS1","[\x1b[32mGIRAPH\x1b[0m][\\u@\\h \\W]\\$ ");
    env::set_var("PATH",format!("{}:{}/bin:{}/sbin",env::var("PATH")?,hadoop_home,hadoop_home)); 

    match cmd!("bash").run(){
        Ok(_) => {},
        Err(e) => println!("error: {}",e),
    }
    cmd!("ssh",&master_node,"stop-mapred.sh").run()?;
    cmd!("ssh",&master_node,"stop-dfs.sh").run()?;
    println!("cleaning storage");
    for i in preserved_nodes.iter(){
        cmd!("ssh",i,"rm -rf /local/ddps2015/giraph").run()?;
        cmd!("ssh",i,"rm -rf /tmp/ddps2015*").run()?;
    }


    println!("exiting");
    Ok(())
}
