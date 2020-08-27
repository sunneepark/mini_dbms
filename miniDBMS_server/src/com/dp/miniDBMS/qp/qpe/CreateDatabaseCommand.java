package com.dp.miniDBMS.qp.qpe;

import com.dp.miniDBMS.cm.SessionManager;
import com.dp.miniDBMS.qp.qpp.SemanticException;
import com.dp.miniDBMS.qp.qpp.Token;
import com.dp.miniDBMS.sm.smm.MemoryDicTBSManager;
import com.dp.miniDBMS.sm.smo.DicDatabase;

public class CreateDatabaseCommand extends Command {
	private String databaseName;
	private boolean useCommand = false;
	
	public CreateDatabaseCommand(Token tok, String databaseName) {
		super(tok);
		this.databaseName = databaseName;
	}
	public CreateDatabaseCommand(Token tok, String databaseName, boolean use) {
		super(tok);
		this.databaseName = databaseName;
		this.useCommand = true;
	}

	@Override
	public String execute() {
		return null;
	}
	@Override
	public String optimize() {
		return null;
	}

	@Override
	public String validate() throws SemanticException {
		if(!useCommand) {
			System.out.println(this.databaseName);
			
			MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
			
			if(mdtbsm.addDatabase(databaseName)) { 
				return "success database created";
			}
			else 
				throw new SemanticException( "duplicate exist dbName");
		}
		else if(useCommand) {
			MemoryDicTBSManager mdtbsm = MemoryDicTBSManager.getInstance();
			DicDatabase db = mdtbsm.isExistDatabase(databaseName);
			if(db == null) {
				throw new SemanticException(" There is no "+databaseName);
			}
			else {
				SessionManager sm = SessionManager.getInstance();
				sm.setDB(getSessionId(), db);
				return "Datbase Changed";
			}
			
		}
		return execute();		
	}

	@Override
	public void archiveLog() {
		// TODO Auto-generated method stub
		
	}
}
