import java.sql.*;
import java.io.*;
import java.math.BigDecimal;

/*
 * quick and dirty hack to bump the sequences after a datapump.
 */

public class SequenceBump{
    public Connection conn;  
    public String url = "jdbc:oracle:thin:@//192.168.5.36:1521/orca";
    public String username = "";
    public String password = "";
    
    public static void main(String args []) throws Exception{
	String url = args[0];
	String username = args[1];
	String password = args[2];
        
	try{
	    SequenceBump connector = new SequenceBump(url, username, password);
	    connector.connect();
            connector.selectBanner(System.out);
            connector.updateSeqs(System.out);
            connector.disconnect();
	}catch (Exception e){
	    e.printStackTrace();
	}
    }

    public SequenceBump(String url, String user, String pass){
	this.url = url;
	this.username = user;
	this.password = pass;
        System.out.println(getClass().getName()+": Connection details: "+this.url +" "+this.username+" "+this.password);
    }

    public void connect() throws Exception {
        try {
	    Class.forName ("oracle.jdbc.driver.OracleDriver");
	    conn = DriverManager.getConnection(url, username, password);
	} catch (Exception e) {
	    e.printStackTrace();
	    throw e;
	}	    
    }

    public void disconnect(){
	try { 
	    conn.close(); 
	} catch (Exception ignore) {}
    }
    
    public void println(Object o, PrintStream redirect){
        if (System.out != redirect)
            System.out.println(getClass().getName()+":"+o);

	redirect.println(getClass().getName()+" : "+o);
	redirect.flush();
    }

    public void updateSeqs(PrintStream _out) throws Exception{        
	Statement statement = null;
        Statement statement2 = null;
        
	try {
	    statement = conn.createStatement();
	    ResultSet rset = statement.executeQuery("SELECT TABLE_NAME from USER_TABLES");
	    try {
                int i=0; 
		while (rset.next()){
                    i++;                   
                    String tname = rset.getString(1);
                    //skip custom sequence tables that have no ID and SYS* tables
                    if (!tname.endsWith("_SEQ") && !tname.endsWith("_SEQUENCE") && !tname.startsWith("SYS") ){ 

                        println("table: "+tname, _out);
                        String seqName = tname+"_SEQ";
                        statement2 = conn.createStatement();
                        try{
                            ResultSet rset2 = statement2.executeQuery("SELECT max(id) from "+tname);                    
                            BigDecimal until = null;
                            while (rset2.next()){
                                until = rset2.getBigDecimal(1);
                                println("until: "+until, _out);
                            } 
                            statement2.close();                
                            if (until != null)
                                selectSeq(_out, seqName, until);
                        }catch(java.sql.SQLSyntaxErrorException e){
                            System.err.println(e.getMessage()); //catch non existent id
                        }                   
                    }

                }
	    } finally {
		try { rset.close(); } catch (Exception ignore) {}
	    }
	} finally {
	    try { statement.close(); } catch (Exception ignore) {}
	}	       
    }

    public void selectSeq(PrintStream _out, String seqName, BigDecimal until)  throws Exception{        
        
	Statement statement = null;
	try {
	    statement = conn.createStatement();
	    ResultSet rset = statement.executeQuery("SELECT "+seqName+".NEXTVAL FROM DUAL");
	    try {
		while (rset.next()){
                    BigDecimal bd = rset.getBigDecimal(1);
                    if (bd != null){
                        if (bd.compareTo(until) >= 0){
                            //println ("bd equals: selected nextval: "+bd + " - "+seqName, _out);   // Print col 1
                            println (bd, _out);   // Print col 1
                        }else{
                            println ("selected: "+bd + " from "+seqName, _out);   // Print col 1
                            selectSeq(_out, seqName, until);
                        }
                    }
                }
	    } finally {
		try { rset.close(); } catch (Exception ignore) {}
	    }
        }catch(java.sql.SQLSyntaxErrorException e){
            System.err.println(e.getMessage()); //catch non existent seq
	} finally {
	    try { statement.close(); } catch (Exception ignore) {}
	}	       
    }


    public void selectBanner(PrintStream _out) throws Exception{		
	Statement statement = null;
	try {
	    statement = conn.createStatement();
	    ResultSet rset = statement.executeQuery("select BANNER from SYS.V_$VERSION");
	    try {
		while (rset.next())
		    println (rset.getString(1), _out);   // Print col 1
	    } finally {
		try { rset.close(); } catch (Exception ignore) {}
	    }
	} finally {
	    try { statement.close(); } catch (Exception ignore) {}
	}	
    }
    
    public void select(String sql, PrintStream _out) throws Exception{		
	Statement statement = null;
	try {
	    statement = conn.createStatement();
	    ResultSet rset = statement.executeQuery(sql);
	    try {
		while (rset.next())
		    println (rset.getString(1), _out);   // Print col 1
	    } finally {
		try { rset.close(); } catch (Exception ignore) {}
	    }
	} finally {
	    try { statement.close(); } catch (Exception ignore) {}
	}	
    }


}
