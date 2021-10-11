/*******************************************************************************
 * Copyright 2017 Observational Health Data Sciences and Informatics
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.jCdmBuilder;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.cdm.Cdm;
import org.ohdsi.jCdmBuilder.cdm.EraBuilder;
import org.ohdsi.jCdmBuilder.etls.cdm.CdmEtl;
import org.ohdsi.jCdmBuilder.vocabulary.CopyVocabularyFromSchema;
import org.ohdsi.jCdmBuilder.vocabulary.InsertVocabularyInServer;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.IniFile;

public class JCdmBuilderMain {
	public static final String VERSION = "0.4.1";
	
	private static final String ICON = "/org/ohdsi/jCdmBuilder/OHDSI Icon Picture 048x048.gif";
	
	private static String					DATABASE_TYPE_POSTGRESQL				= "PostgreSQL";
	private static String					DATABASE_TYPE_ORACLE					= "Oracle";
	private static String					DATABASE_TYPE_SQLSERVER					= "SQL Server";
	private static String					DATABASE_TYPE_MYSQL						= "MySQL";
	
	private static String[]					DATABASE_TYPES							= new String[] { DATABASE_TYPE_POSTGRESQL, DATABASE_TYPE_ORACLE, DATABASE_TYPE_SQLSERVER };
	private static HashSet<String>			BULK_LOAD_DATABASE_TYPES				= new HashSet<String>() {
																						private static final long serialVersionUID = 946936162824903605L;
																						{
																							add(DATABASE_TYPE_POSTGRESQL);
																							add(DATABASE_TYPE_SQLSERVER);
																						}};
	
	private static String					SOURCEFOLDER					        = "source folder";
	private static String					SOURCEVIASERVERFOLDER			        = "source via server folder";
	private static String					SOURCEDATABASE					        = "source database";
	private static String					VOCABFOLDER						        = "vocab folder";
	private static String					VOCABVIASERVERFOLDER                    = "vocab via server folder";
	private static String					VOCABSCHEMA						        = "vocab schema";
	
	private static String					VOCABTYPE_LOAD							= "1. Load ATHENA CSV files to server";
	private static String					VOCABTYPE_BULK_LOAD						= "2. Bulk Load ATHENA CSV files from server to server";
	private static String					VOCABTYPE_SCHEMA_LOAD					= "3. Load vocabulary from schema";
	
	private static String					ETLTYPE_LOAD							= "1. Load CSV files in CDM format to server";
	private static String					ETLTYPE_BULK_LOAD						= "2. Bulk Load CSV files from server in CDM format to server";
	
	public static List<String> 				errors = null;
	
	private JFrame							frame;
	private JTabbedPane						tabbedPane;
	private JTextField						folderField;
	private JComboBox<String>				vocabSourceType;
	private DefaultComboBoxModel<String>	vocabSourceTypeModel;
	private JTextField						vocabFolderField;
	private JTextField						vocabServerFolderField;
	private JTextField						vocabServerTempFolderField;
	private JTextField						vocabServerTempLocalFolderField;
	private JTextField						vocabSchemaField;
	private JPanel							vocabCards;
	private JCheckBox						executeStructureCheckBox;
	private JCheckBox						executeVocabCheckBox;
	private JCheckBox						executeETLCheckBox;
	private JCheckBox           			continueOnErrorCheckBox;
	private JCheckBox						executeIndicesCheckBox;
	private JCheckBox						executeConstraintsCheckBox;
	private JCheckBox						executeConditionErasCheckBox;
	private JCheckBox						executeDrugErasCheckBox;
	private JCheckBox						executeResultsStructureCheckBox;
	private JCheckBox						executeResultsDataCheckBox;
	private JCheckBox						executeResultsIndicesCheckBox;
	private JComboBox<String>				etlType;
	private DefaultComboBoxModel<String>	etlTypeModel;
	private JComboBox<String>				targetType;
	private JTextField						versionIdField;
	private JTextField						targetUserField;
	private JTextField						targetPasswordField;
	private JTextField						targetServerField;
	private JTextField						targetSchemaField;
	private JTextField						targetResultsSchemaField;
	private JComboBox<String>				targetCdmVersion;
	//private JComboBox<String>				sourceType;
	private JTextField						sourceDelimiterField;
	private JTextField						sourceQuoteField;
	private JTextField						sourceNullValueField;
	private JTextField						sourceFolderField;
	//private JTextField					sourceServerField;
	//private JTextField					sourceUserField;
	//private JTextField					sourcePasswordField;
	//private JTextField					sourceDatabaseField;
	private JTextField						sourceServerDelimiterField;
	private JTextField						sourceServerQuoteField;
	private JTextField						sourceServerNullValueField;
	private JTextField						sourceServerFolderField;
	private JTextField						sourceServerTempFolderField;
	private JTextField						sourceServerTempLocalFolderField;
	private JPanel							sourceCards;
	private boolean							executeCdmStructureWhenReady		    = false;
	private boolean							executeVocabWhenReady				    = false;
	private boolean							executeEtlWhenReady					    = false;
	private boolean							executeIndicesWhenReady				    = false;
	private boolean							executeConstraintsWhenReady			    = false;
	private boolean							executeConditionErasWhenReady		    = false;
	private boolean							executeDrugErasWhenReady			    = false;
	private boolean							executeResultsStructureWhenReady	    = false;
	private boolean							executeResultsDataWhenReady		        = false;
	private boolean							executeResultsIndicesWhenReady		    = false;
	private boolean							idsToBigInt							    = false;
	private boolean             			continueOnError                         = false;
	private IniFile							settingsFile							= null;
	
	private boolean							fieldLinkActive							= false;
	private List<JComponent>				componentsToDisableWhenRunning	        = new ArrayList<JComponent>();
	private boolean             			autoStart                               = false;
	
	
	public static void main(String[] args) {
		JCdmBuilderMain.errors = new ArrayList<String>();
		new JCdmBuilderMain(args);
	}
	

	/**
	 * Sets an icon on a JFrame or a JDialog.
	 * @param container - the GUI component on which the icon is to be put
	 */
	public static void setIcon(Object container){
		URL url = JCdmBuilderMain.class.getResource(ICON);
		Image img = Toolkit.getDefaultToolkit().getImage(url);
		if (container.getClass() == JFrame.class ||
				JFrame.class.isAssignableFrom(container.getClass()))
			((JFrame)container).setIconImage(img);
		else if (container.getClass() == JDialog.class  ||
				JDialog.class.isAssignableFrom(container.getClass()))
			((JDialog)container).setIconImage(img);
		else
			((JFrame)container).setIconImage(img);
	}
	
	
	public JCdmBuilderMain(String[] args) {
		if (args.length > 0 && (args[0].toLowerCase().equals("-usage") || args[0].toLowerCase().equals("-help") || args[0].toLowerCase().equals("?"))) {
			printUsage();
			return;
		}
		frame = new JFrame("JCDMBuilder version " + VERSION);
		
		frame.addWindowListener(new WindowAdapter() {
			public void windowClosing(WindowEvent e) {
				System.exit(0);
			}
		});
		frame.setLayout(new BorderLayout());
		JCdmBuilderMain.setIcon(frame);
		
		JMenuBar menuBar = createMenu();
		
		JComponent tabsPanel = createTabsPanel();
		JComponent consolePanel = createConsolePanel();
		
		frame.add(consolePanel, BorderLayout.CENTER);
		frame.add(tabsPanel, BorderLayout.NORTH);
		frame.setJMenuBar(menuBar);
		
		frame.pack();
		frame.setVisible(true);
		ObjectExchange.frame = frame;
		executeParameters(args);
		if (	executeCdmStructureWhenReady ||  
				executeVocabWhenReady || 
				executeEtlWhenReady || 
				executeIndicesWhenReady || 
				executeConstraintsWhenReady || 
				executeConditionErasWhenReady || 
				executeDrugErasWhenReady || 
				executeConditionErasWhenReady || 
				executeResultsStructureWhenReady ||
				executeResultsDataWhenReady ||
				executeResultsIndicesWhenReady) {
			autoStart = true;
			ObjectExchange.console.setDebugFile(folderField.getText() + "/Console.txt");
			AutoRunThread autoRunThread = new AutoRunThread();
			autoRunThread.start();
		}
		
	}
	
	
	private JMenuBar createMenu() {
		JMenuBar menuBar = new JMenuBar();
		JMenu file = new JMenu("File");
		
		JMenuItem loadMenuItem = new JMenuItem("Load settings");
		loadMenuItem.setToolTipText("Load Settings");
		loadMenuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				loadSettingsFile();
			}
		});
		file.add(loadMenuItem);
		
		JMenuItem saveMenuItem = new JMenuItem("Save settings");
		saveMenuItem.setToolTipText("Save Settings");
		saveMenuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				saveSettingsFile();
			}
		});
		file.add(saveMenuItem);
		
		JMenuItem exitMenuItem = new JMenuItem("Exit");
		exitMenuItem.setToolTipText("Exit application");
		exitMenuItem.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				System.exit(0);
			}
		});
		file.add(exitMenuItem);
		
		menuBar.add(file);
		return menuBar;
	}
	
	
	private JComponent createTabsPanel() {
		tabbedPane = new JTabbedPane();
		
		JPanel locationPanel = createLocationsPanel();
		tabbedPane.addTab("Locations", null, locationPanel, "Specify the location of the source data, the CDM, and the working folder");
		
		JPanel vocabPanel = createVocabPanel();
		tabbedPane.addTab("Vocabulary", null, vocabPanel, "Upload the vocabulary to the server");
		
		JPanel etlPanel = createEtlPanel();
		tabbedPane.addTab("ETL", null, etlPanel, "Extract, Transform and Load the data into the OMOP CDM");
		
		JPanel executePanel = createExecutePanel();
		tabbedPane.addTab("Execute", null, executePanel, "Run multiple steps automatically");
		
		// Link fields
		vocabFolderField.getDocument().addDocumentListener(new TextFieldLink(vocabFolderField, vocabServerFolderField));
		vocabServerFolderField.getDocument().addDocumentListener(new TextFieldLink(vocabServerFolderField, vocabFolderField));
		vocabServerTempFolderField.getDocument().addDocumentListener(new TextFieldLink(vocabServerTempFolderField, sourceServerTempFolderField));
		sourceServerTempFolderField.getDocument().addDocumentListener(new TextFieldLink(sourceServerTempFolderField, vocabServerTempFolderField));
		vocabServerTempLocalFolderField.getDocument().addDocumentListener(new TextFieldLink(vocabServerTempLocalFolderField, sourceServerTempLocalFolderField));
		sourceServerTempLocalFolderField.getDocument().addDocumentListener(new TextFieldLink(sourceServerTempLocalFolderField, vocabServerTempLocalFolderField));
		sourceFolderField.getDocument().addDocumentListener(new TextFieldLink(sourceFolderField, sourceServerFolderField));
		sourceServerFolderField.getDocument().addDocumentListener(new TextFieldLink(sourceServerFolderField, sourceFolderField));
		sourceDelimiterField.getDocument().addDocumentListener(new TextFieldLink(sourceDelimiterField, sourceServerDelimiterField));
		sourceServerDelimiterField.getDocument().addDocumentListener(new TextFieldLink(sourceServerDelimiterField, sourceDelimiterField));
		sourceQuoteField.getDocument().addDocumentListener(new TextFieldLink(sourceQuoteField, sourceServerQuoteField));
		sourceServerQuoteField.getDocument().addDocumentListener(new TextFieldLink(sourceServerQuoteField, sourceQuoteField));
		sourceNullValueField.getDocument().addDocumentListener(new TextFieldLink(sourceNullValueField, sourceServerNullValueField));
		sourceServerNullValueField.getDocument().addDocumentListener(new TextFieldLink(sourceServerNullValueField, sourceNullValueField));
		
		return tabbedPane;
	}
	
	
	private JPanel createLocationsPanel() {
		JPanel panel = new JPanel();
		
		panel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;
		
		JPanel folderPanel = new JPanel();
		folderPanel.setLayout(new BoxLayout(folderPanel, BoxLayout.X_AXIS));
		folderPanel.setBorder(BorderFactory.createTitledBorder("Working folder"));
		folderField = new JTextField();
		folderField.setText("");
		folderField.setToolTipText("The folder where all output will be written");
		folderPanel.add(folderField);
		JButton pickButton = new JButton("Select");
		pickButton.setToolTipText("Pick a different working folder");
		folderPanel.add(pickButton);
		pickButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickFolder();
			}
		});
		componentsToDisableWhenRunning.add(pickButton);
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 1;
		panel.add(folderPanel, c);
		
		JPanel targetPanel = new JPanel();
		targetPanel.setLayout(new GridLayout(0, 2));
		targetPanel.setBorder(BorderFactory.createTitledBorder("Target CDM location"));
		targetPanel.add(new JLabel("Database type"));
		targetType = new JComboBox<String>(DATABASE_TYPES);
		targetType.setToolTipText("Select the type of server where the CDM and vocabulary will be stored");
		targetPanel.add(targetType);
		targetPanel.add(new JLabel("Server location"));
		targetServerField = new JTextField("");
		targetPanel.add(targetServerField);
		targetPanel.add(new JLabel("User name"));
		targetUserField = new JTextField("");
		targetPanel.add(targetUserField);
		targetPanel.add(new JLabel("Password"));
		targetPasswordField = new JPasswordField("");
		targetPanel.add(targetPasswordField);
		targetPanel.add(new JLabel("CDM Schema name"));
		targetSchemaField = new JTextField("");
		targetPanel.add(targetSchemaField);
		targetPanel.add(new JLabel("Results Schema name"));
		targetResultsSchemaField = new JTextField("");
		targetPanel.add(targetResultsSchemaField);
		targetPanel.add(new JLabel("CDM version"));
		targetCdmVersion = new JComboBox<String>(Cdm.availableVersions);
		targetCdmVersion.setToolTipText("Select the CMD version");
		targetCdmVersion.setSelectedIndex(2);
		
		targetType.addItemListener(new ItemListener() {
			
			@Override
			public void itemStateChanged(ItemEvent arg0) {
				updateETLType();
				updateVocabSourceType();
				
				if (arg0.getItem().toString().equals("Oracle")) {
					targetServerField.setToolTipText("For Oracle servers this field contains the SID, servicename, and optionally the port: '<host>/<sid>', '<host>:<port>/<sid>', '<host>/<service name>', or '<host>:<port>/<service name>'.");
					targetUserField.setToolTipText("For Oracle servers this field contains the name of the user used to log in.");
					targetPasswordField.setToolTipText("For Oracle servers this field contains the password corresponding to the user.");
					targetSchemaField.setToolTipText("For Oracle servers this field contains the schema (i.e. 'user' in Oracle terms) containing the target tables. The user will be created with the same password.");
				} else if (arg0.getItem().toString().equals("PostgreSQL")) {
					targetServerField.setToolTipText("For PostgreSQL servers this field contains the host name and database name (<host>/<database>).");
					targetUserField.setToolTipText("The user used to log in to the server.");
					targetPasswordField.setToolTipText("The password used to log in to the server.");
					targetSchemaField.setToolTipText("For PostgreSQL servers this field contains the schema containing the target tables.");
				} else if (arg0.getItem().toString().equals("SQL Server")) {
					targetServerField.setToolTipText("For Microsoft SQL Server this field contains the server address, the database and optionally the port: '<host>:<port>;database=<database>;', or  '<host>;database=<database>;'");
					targetUserField.setToolTipText("The user used to log in to the server. Optionally, the domain can be specified as <domain>/<user> (e.g. 'MyDomain/Joe').");
					targetPasswordField.setToolTipText("The password used to log in to the server.");
					targetSchemaField.setToolTipText("The name of the schema containing the target tables.");
				} else {
					targetServerField.setToolTipText("This field contains the name or IP-address or name of the database server.");
					targetUserField.setToolTipText("The user used to log in to the server.");
					targetPasswordField.setToolTipText("The password used to log in to the server.");
					targetSchemaField.setToolTipText("The name of the database containing the target tables.");
				}
			}
		});
		targetType.setSelectedIndex(1);
		targetPanel.add(targetCdmVersion);
		
		c.gridx = 0;
		c.gridy = 1;
		c.gridwidth = 1;
		panel.add(targetPanel, c);
		
		JPanel testConnectionButtonPanel = new JPanel();
		testConnectionButtonPanel.setLayout(new BoxLayout(testConnectionButtonPanel, BoxLayout.X_AXIS));
		testConnectionButtonPanel.add(Box.createHorizontalGlue());
		
		JButton testConnectionButton = new JButton("Test connection");
		testConnectionButton.setBackground(new Color(151, 220, 141));
		testConnectionButton.setToolTipText("Test the connection");
		testConnectionButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				testConnection(getTargetDbSettings());
			}
		});
		componentsToDisableWhenRunning.add(testConnectionButton);
		testConnectionButtonPanel.add(testConnectionButton);
		
		c.gridx = 0;
		c.gridy = 2;
		c.gridwidth = 1;
		panel.add(testConnectionButtonPanel, c);
		
		return panel;
	}
	
	
	private void testConnection(DbSettings dbSettings) {
		int messageType = JOptionPane.ERROR_MESSAGE;
		String messageTitle = "Error connecting to server";
		String message = testConnectionResult(dbSettings);
		if (message.equals("OK")) {
			messageType = JOptionPane.INFORMATION_MESSAGE;
			messageTitle = "Connection succesful";
			message = "Succesfully connected to " + dbSettings.cdmSchema + " on server " + dbSettings.server;
		}
		JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), messageTitle, messageType);
	}
	
	
	private String testConnectionResult(DbSettings dbSettings) {
		String result = "OK";
		if (dbSettings.server == null || dbSettings.server.equals(""))  result = "Please specify the server";
		else {
			RichConnection connection;
			try {
				connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);

				try {
					connection.getTableNames(dbSettings.cdmSchema);
					connection.close();
				} catch (Exception e) {
					result = "Could not connect to database: " + e.getMessage();
				}
			} catch (Exception e) {
				result = "Could not connect: " + e.getMessage();
			}
		}
		
		return result;
	}
	
	
	private void loadSettings(String fileName) {
		settingsFile = new IniFile(fileName);
		settingsFile.readFile();
		
		// Locations
		if (settingsFile.getValue("Locations", "Workspace Folder")           != null) folderField.setText(settingsFile.getValue("Locations", "Workspace Folder"));
		if (settingsFile.getValue("Locations", "Target Database Type")       != null) targetType.setSelectedItem(settingsFile.getValue("Locations", "Target Database Type"));
		if (settingsFile.getValue("Locations", "Target Server Location")     != null) targetServerField.setText(settingsFile.getValue("Locations", "Target Server Location"));
		if (settingsFile.getValue("Locations", "Target User Name")           != null) targetUserField.setText(settingsFile.getValue("Locations", "Target User Name"));
		if (settingsFile.getValue("Locations", "Target CDM Schema Name")     != null) targetSchemaField.setText(settingsFile.getValue("Locations", "Target CDM Schema Name"));
		if (settingsFile.getValue("Locations", "Target Results Schema Name") != null) targetResultsSchemaField.setText(settingsFile.getValue("Locations", "Target Results Schema Name"));
		if (settingsFile.getValue("Locations", "Target CDM Version")         != null) targetCdmVersion.setSelectedItem(settingsFile.getValue("Locations", "Target CDM Version"));
		
		// Vocabulary
		if (settingsFile.getValue("Vocabulary", "Source Type")               != null) vocabSourceType.setSelectedItem(settingsFile.getValue("Vocabulary", "Source Type"));
		if (settingsFile.getValue("Vocabulary", "Source Folder")             != null) vocabFolderField.setText(settingsFile.getValue("Vocabulary", "Source Folder"));
		if (settingsFile.getValue("Vocabulary", "Source Schema")             != null) vocabSchemaField.setText(settingsFile.getValue("Vocabulary", "Source Schema"));
		
		// ETL
		if (settingsFile.getValue("ETL", "ETL Type")                         != null) etlType.setSelectedItem(settingsFile.getValue("ETL", "ETL Type"));
		if (settingsFile.getValue("ETL", "Version ID")                       != null) versionIdField.setText(settingsFile.getValue("ETL", "Version ID"));
		if (settingsFile.getValue("ETL", "Source Folder")                    != null) sourceFolderField.setText(settingsFile.getValue("ETL", "Source Folder"));
		if (settingsFile.getValue("ETL", "Source Delimiter")                 != null) sourceDelimiterField.setText(settingsFile.getValue("ETL", "Source Delimiter"));
		if (settingsFile.getValue("ETL", "Source Quote")                     != null) sourceQuoteField.setText(settingsFile.getValue("ETL", "Source Quote"));
		if (settingsFile.getValue("ETL", "Source Null Value")                != null) sourceNullValueField.setText(settingsFile.getValue("ETL", "Source Null Value"));
		if (settingsFile.getValue("ETL", "Server Temp Folder")               != null) sourceServerTempFolderField.setText(settingsFile.getValue("ETL", "Server Temp Folder"));
		if (settingsFile.getValue("ETL", "Local Path Server Temp Folder")    != null) sourceServerTempLocalFolderField.setText(settingsFile.getValue("ETL", "Local Path Server Temp Folder"));
	}
	
	
	private void saveSettings(String fileName) {
		settingsFile = new IniFile(fileName);
		
		// Locations
		settingsFile.addGroup("Locations", null);
		settingsFile.setValue("Locations", "Workspace Folder", folderField.getText(), null);
		settingsFile.setValue("Locations", "Target Database Type", targetType.getSelectedItem().toString(), null);
		settingsFile.setValue("Locations", "Target Server Location", targetServerField.getText(), null);
		settingsFile.setValue("Locations", "Target User Name", targetUserField.getText(), null);
		settingsFile.setValue("Locations", "Target CDM Schema Name", targetSchemaField.getText(), null);
		settingsFile.setValue("Locations", "Target Results Schema Name", targetResultsSchemaField.getText(), null);
		settingsFile.setValue("Locations", "Target CDM Version", targetCdmVersion.getSelectedItem().toString(), null);
		
		// Vocabulary
		settingsFile.addGroup("Vocabulary", null);
		settingsFile.setValue("Vocabulary", "Source Type", vocabSourceType.getSelectedItem().toString(), null);
		settingsFile.setValue("Vocabulary", "Source Folder", vocabFolderField.getText(), null);
		settingsFile.setValue("Vocabulary", "Source Schema", vocabSchemaField.getText(), null);
		
		// ETL
		settingsFile.addGroup("ETL", null);
		settingsFile.setValue("ETL", "ETL Type", etlType.getSelectedItem().toString(), null);
		settingsFile.setValue("ETL", "Version ID", versionIdField.getText(), null);
		settingsFile.setValue("ETL", "Source Folder", sourceFolderField.getText(), null);
		settingsFile.setValue("ETL", "Source Delimiter", sourceDelimiterField.getText(), null);
		settingsFile.setValue("ETL", "Source Quote", sourceQuoteField.getText(), null);
		settingsFile.setValue("ETL", "Source Null Value", sourceNullValueField.getText(), null);
		settingsFile.setValue("ETL", "Server Temp Folder", sourceServerTempFolderField.getText(), null);
		settingsFile.setValue("ETL", "Local Path Server Temp Folder", sourceServerTempLocalFolderField.getText(), null);
		
		settingsFile.writeFile();
	}
	
	
	private JPanel createVocabPanel() {
		JPanel panel = new JPanel(new BorderLayout());
		
		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;
		
		JPanel vocabSourceTypePanel = new JPanel();
		vocabSourceTypePanel.setLayout(new BoxLayout(vocabSourceTypePanel, BoxLayout.X_AXIS));
		vocabSourceTypePanel.setBorder(BorderFactory.createTitledBorder("Vocabulary Source type"));
		vocabSourceTypeModel = new DefaultComboBoxModel<String>(new String[] {});
		vocabSourceType = new JComboBox<String>(vocabSourceTypeModel);
		updateVocabSourceType();
		vocabSourceType.setToolTipText("Select the appropriate vocabulary load process");
		vocabSourceType.addItemListener(new ItemListener() {
			
			@Override
			public void itemStateChanged(ItemEvent arg0) {
				String selection = arg0.getItem().toString();
				if (selection.equals(VOCABTYPE_LOAD))
					((CardLayout) vocabCards.getLayout()).show(vocabCards, VOCABFOLDER);
				else if (selection.equals(VOCABTYPE_BULK_LOAD))
						((CardLayout) vocabCards.getLayout()).show(vocabCards, VOCABVIASERVERFOLDER);
				else
					((CardLayout) vocabCards.getLayout()).show(vocabCards, VOCABSCHEMA);
			}
		});
		vocabSourceTypePanel.add(vocabSourceType);
		vocabSourceTypePanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, vocabSourceTypePanel.getPreferredSize().height));
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 1;
		mainPanel.add(vocabSourceTypePanel, c);
		
		
		// Vocabulary Source Type 1 Panel
		JPanel vocabFolderPanel = new JPanel();
		vocabFolderPanel.setLayout(new GridLayout(6, 2));
		vocabFolderPanel.setBorder(BorderFactory.createTitledBorder("Vocabulary source folder location"));
		
		vocabFolderPanel.add(new JLabel("Folder"));
		
		JPanel vocabFolderFieldPanel = new JPanel();
		vocabFolderFieldPanel.setLayout(new BoxLayout(vocabFolderFieldPanel, BoxLayout.X_AXIS));
		vocabFolderField = new JTextField();
		vocabFolderField.setText("");
		vocabFolderField.setToolTipText("Specify the name of the folder containing the vocabulary CSV files here");
		vocabFolderFieldPanel.add(vocabFolderField);
		JButton pickVocabFolderButton = new JButton("Pick folder");
		pickVocabFolderButton.setToolTipText("Select the folder containing the vocabulary CSV files");
		vocabFolderFieldPanel.add(pickVocabFolderButton);
		pickVocabFolderButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickVocabFolder();
			}
		});
		vocabFolderPanel.add(vocabFolderFieldPanel);
		
		vocabFolderPanel.add(Box.createHorizontalGlue());
		vocabFolderPanel.add(Box.createHorizontalGlue());
		vocabFolderPanel.add(Box.createHorizontalGlue());
		vocabFolderPanel.add(Box.createHorizontalGlue());
		vocabFolderPanel.add(Box.createHorizontalGlue());
		vocabFolderPanel.add(Box.createHorizontalGlue());
		
		
		// Vocabulary Source Type 2 Panel
		JPanel vocabServerFolderPanel = new JPanel();
		vocabServerFolderPanel.setLayout(new GridLayout(6, 2));
		vocabServerFolderPanel.setBorder(BorderFactory.createTitledBorder("Vocabulary source folder locations"));
		
		vocabServerFolderPanel.add(new JLabel("Folder"));
		JPanel vocabViaServerFolderFieldPanel = new JPanel();
		vocabViaServerFolderFieldPanel.setLayout(new BoxLayout(vocabViaServerFolderFieldPanel, BoxLayout.X_AXIS));
		vocabServerFolderField = new JTextField();
		vocabServerFolderField.setText("");
		vocabServerFolderField.setToolTipText("Specify the name of the folder containing the CSV files here");
		vocabViaServerFolderFieldPanel.add(vocabServerFolderField);
		JButton pickvocabViaServerFolderButton = new JButton("Pick folder");
		pickvocabViaServerFolderButton.setToolTipText("Select the folder containing the vocab CSV files");
		vocabViaServerFolderFieldPanel.add(pickvocabViaServerFolderButton);
		pickvocabViaServerFolderButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickVocabFolder();
			}
		});
		vocabServerFolderPanel.add(vocabViaServerFolderFieldPanel);
		
		vocabServerFolderPanel.add(new JLabel("Server folder"));
		JPanel vocabServerTempFolderFieldPanel = new JPanel();
		vocabServerTempFolderFieldPanel.setLayout(new BoxLayout(vocabServerTempFolderFieldPanel, BoxLayout.X_AXIS));
		vocabServerTempFolderField = new JTextField();
		vocabServerTempFolderField.setText("");
		vocabServerTempFolderField.setToolTipText("Specify the path of the temporary folder on the server here");
		vocabServerTempFolderFieldPanel.add(vocabServerTempFolderField);
		JButton pickServerFolderButton = new JButton("Pick folder");
		pickServerFolderButton.setToolTipText("Select the path of the temporary folder on the server");
		vocabServerTempFolderFieldPanel.add(pickServerFolderButton);
		pickServerFolderButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickTemporaryServerFolder();
			}
		});
		vocabServerFolderPanel.add(vocabServerTempFolderFieldPanel);

		vocabServerTempLocalFolderField = new JTextField();
		vocabServerTempLocalFolderField.setText("");
		vocabServerTempLocalFolderField.setToolTipText("Specify the local path on the server of the temporary folder on the server here");
		vocabServerFolderPanel.add(new JLabel("Local path server folder"));
		vocabServerFolderPanel.add(vocabServerTempLocalFolderField);
		
		vocabServerFolderPanel.add(Box.createHorizontalGlue());
		vocabServerFolderPanel.add(Box.createHorizontalGlue());
		vocabServerFolderPanel.add(Box.createHorizontalGlue());
		vocabServerFolderPanel.add(Box.createHorizontalGlue());
		
		
		// Vocabulary Source Type 3 Panel
		JPanel vocabSchemaPanel = new JPanel();
		vocabSchemaPanel.setLayout(new GridLayout(6, 2));
		vocabSchemaPanel.setBorder(BorderFactory.createTitledBorder("Vocabulary source schema"));

		vocabSchemaPanel.add(new JLabel("Schema"));
		vocabSchemaField = new JTextField();
		vocabSchemaField.setText("");
		vocabSchemaField.setToolTipText("Specify the name of the schema containing the vocabulary tables here");
		vocabSchemaPanel.add(vocabSchemaField);
		
		vocabSchemaPanel.add(Box.createHorizontalGlue());
		vocabSchemaPanel.add(Box.createHorizontalGlue());
		vocabSchemaPanel.add(Box.createHorizontalGlue());
		vocabSchemaPanel.add(Box.createHorizontalGlue());
		vocabSchemaPanel.add(Box.createHorizontalGlue());
		vocabSchemaPanel.add(Box.createHorizontalGlue());
		
		vocabCards = new JPanel(new CardLayout());
		vocabCards.add(vocabFolderPanel, VOCABFOLDER);
		vocabCards.add(vocabServerFolderPanel, VOCABVIASERVERFOLDER);
		vocabCards.add(vocabSchemaPanel, VOCABSCHEMA);
		
		c.gridx = 0;
		c.gridy = 1;
		c.gridwidth = 2;
		mainPanel.add(vocabCards, c);
		vocabCards.setMaximumSize(new Dimension(Integer.MAX_VALUE, vocabCards.getPreferredSize().height));
		
		panel.add(mainPanel, BorderLayout.NORTH);
		
		return panel;
	}
	
	
	private void updateVocabSourceType() {
		if ((targetType != null) && (vocabSourceType != null)) {
			String currentETLType = (String) vocabSourceType.getSelectedItem();
			vocabSourceTypeModel.removeAllElements();
			vocabSourceTypeModel.addElement(VOCABTYPE_LOAD);
			boolean bulkLoadPossible = false;
			if (BULK_LOAD_DATABASE_TYPES.contains((String) targetType.getSelectedItem())) {
				vocabSourceTypeModel.addElement(VOCABTYPE_BULK_LOAD);
				bulkLoadPossible = true;
			}
			vocabSourceTypeModel.addElement(VOCABTYPE_SCHEMA_LOAD);
			
			if (currentETLType != null) {
				if ((!bulkLoadPossible) && (currentETLType.equals(VOCABTYPE_BULK_LOAD))) {
					vocabSourceType.setSelectedItem(VOCABTYPE_LOAD);
				}
				else {
					vocabSourceType.setSelectedItem(currentETLType);
				}
			}
		}
	}
	
	
	private JPanel createEtlPanel() {
		JPanel panel = new JPanel(new BorderLayout());
		
		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;
		
		JPanel etlTypePanel = new JPanel();
		etlTypePanel.setLayout(new BoxLayout(etlTypePanel, BoxLayout.X_AXIS));
		etlTypePanel.setBorder(BorderFactory.createTitledBorder("ETL type"));
		etlTypeModel = new DefaultComboBoxModel<String>(new String[] {});
		etlType = new JComboBox<String>(etlTypeModel);
		updateETLType();
		etlType.setToolTipText("Select the appropriate ETL process");
		etlType.addItemListener(new ItemListener() {
			
			@Override
			public void itemStateChanged(ItemEvent arg0) {
				String selection = arg0.getItem().toString();
				if (selection.equals(ETLTYPE_LOAD))
					((CardLayout) sourceCards.getLayout()).show(sourceCards, SOURCEFOLDER);
				else if (selection.equals(ETLTYPE_BULK_LOAD))
						((CardLayout) sourceCards.getLayout()).show(sourceCards, SOURCEVIASERVERFOLDER);
				else
					((CardLayout) sourceCards.getLayout()).show(sourceCards, SOURCEDATABASE);
			}
		});
		etlTypePanel.add(etlType);
		etlTypePanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, etlTypePanel.getPreferredSize().height));
		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 1;
		mainPanel.add(etlTypePanel, c);
		
		// Ugly but needed for ETLs at JnJ: Allow user to specify a version ID to insert into _version table:
		JPanel versionIdPanel = new JPanel();
		versionIdPanel.setLayout(new BoxLayout(versionIdPanel, BoxLayout.X_AXIS));
		versionIdPanel.setBorder(BorderFactory.createTitledBorder("Version ID"));
		versionIdField = new JTextField("1");
		versionIdField.setToolTipText("(Optionally:) Specify a version ID (integer) to insert into the unofficial _version table");
		versionIdPanel.add(versionIdField);
		versionIdPanel.setMaximumSize(new Dimension(Integer.MAX_VALUE, versionIdPanel.getPreferredSize().height));
		c.gridx = 1;
		c.gridy = 0;
		c.gridwidth = 1;
		mainPanel.add(versionIdPanel, c);
		
		
		// ETL-Type 1 Panel
		JPanel sourceFolderPanel = new JPanel();
		sourceFolderPanel.setLayout(new GridLayout(6, 2));
		sourceFolderPanel.setBorder(BorderFactory.createTitledBorder("Source folder location"));
		
		sourceFolderPanel.add(new JLabel("Folder"));
		
		JPanel sourceFolderFieldPanel = new JPanel();
		sourceFolderFieldPanel.setLayout(new BoxLayout(sourceFolderFieldPanel, BoxLayout.X_AXIS));
		sourceFolderField = new JTextField();
		sourceFolderField.setText("");
		sourceFolderField.setToolTipText("Specify the name of the folder containing the CSV files here");
		sourceFolderFieldPanel.add(sourceFolderField);
		JButton pickSourceFolderButton = new JButton("Pick folder");
		pickSourceFolderButton.setToolTipText("Select the folder containing the source CSV files");
		sourceFolderFieldPanel.add(pickSourceFolderButton);
		pickSourceFolderButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickSourceFolder();
			}
		});
		sourceFolderPanel.add(sourceFolderFieldPanel);
		
		sourceFolderPanel.add(new JLabel("Delimiter"));
		sourceDelimiterField = new JTextField(",");
		sourceDelimiterField.setToolTipText("The delimiter that separates values. Enter 'tab' for tab.");
		sourceFolderPanel.add(sourceDelimiterField);
		
		sourceFolderPanel.add(new JLabel("Quote"));
		sourceQuoteField = new JTextField("\"");
		sourceQuoteField.setToolTipText("The field quote.");
		sourceFolderPanel.add(sourceQuoteField);
		
		sourceFolderPanel.add(new JLabel("Null value"));
		sourceNullValueField = new JTextField("");
		sourceNullValueField.setToolTipText("The value that represents the NULL value.");
		sourceFolderPanel.add(sourceNullValueField);
		
		sourceFolderPanel.add(Box.createHorizontalGlue());
		sourceFolderPanel.add(Box.createHorizontalGlue());
		sourceFolderPanel.add(Box.createHorizontalGlue());
		

		// ETL-Type 2 Panel
		JPanel sourceServerFolderPanel = new JPanel();
		sourceServerFolderPanel.setLayout(new GridLayout(6, 2));
		sourceServerFolderPanel.setBorder(BorderFactory.createTitledBorder("Source Folder locations"));
		
		sourceServerFolderPanel.add(new JLabel("Folder"));
		JPanel sourceViaServerFolderFieldPanel = new JPanel();
		sourceViaServerFolderFieldPanel.setLayout(new BoxLayout(sourceViaServerFolderFieldPanel, BoxLayout.X_AXIS));
		sourceServerFolderField = new JTextField();
		sourceServerFolderField.setText("");
		sourceServerFolderField.setToolTipText("Specify the name of the folder containing the CSV files here");
		sourceViaServerFolderFieldPanel.add(sourceServerFolderField);
		JButton picksourceViaServerFolderButton = new JButton("Pick folder");
		picksourceViaServerFolderButton.setToolTipText("Select the folder containing the source CSV files");
		sourceViaServerFolderFieldPanel.add(picksourceViaServerFolderButton);
		picksourceViaServerFolderButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickSourceFolder();
			}
		});
		sourceServerFolderPanel.add(sourceViaServerFolderFieldPanel);
		
		sourceServerFolderPanel.add(new JLabel("Delimiter"));
		sourceServerDelimiterField = new JTextField(",");
		sourceServerDelimiterField.setToolTipText("The delimiter that separates values. Enter 'tab' for tab.");
		sourceServerFolderPanel.add(sourceServerDelimiterField);
		
		sourceServerFolderPanel.add(new JLabel("Quote"));
		sourceServerQuoteField = new JTextField("\"");
		sourceServerQuoteField.setToolTipText("The field quote.");
		sourceServerFolderPanel.add(sourceServerQuoteField);
		
		sourceServerFolderPanel.add(new JLabel("Null value"));
		sourceServerNullValueField = new JTextField("");
		sourceServerNullValueField.setToolTipText("The value that represents the NULL value.");
		sourceServerFolderPanel.add(sourceServerNullValueField);
		
		sourceServerFolderPanel.add(new JLabel("Server folder"));
		JPanel sourceServerTempFolderFieldPanel = new JPanel();
		sourceServerTempFolderFieldPanel.setLayout(new BoxLayout(sourceServerTempFolderFieldPanel, BoxLayout.X_AXIS));
		sourceServerTempFolderField = new JTextField();
		sourceServerTempFolderField.setText("");
		sourceServerTempFolderField.setToolTipText("Specify the path of the temporary folder on the server here");
		sourceServerTempFolderFieldPanel.add(sourceServerTempFolderField);
		JButton pickServerFolderButton = new JButton("Pick folder");
		pickServerFolderButton.setToolTipText("Select the path of the temporary folder on the server");
		sourceServerTempFolderFieldPanel.add(pickServerFolderButton);
		pickServerFolderButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickTemporaryServerFolder();
			}
		});
		sourceServerFolderPanel.add(sourceServerTempFolderFieldPanel);

		sourceServerTempLocalFolderField = new JTextField();
		sourceServerTempLocalFolderField.setText("");
		sourceServerTempLocalFolderField.setToolTipText("Specify the local path on the server of the temporary folder on the server here");
		sourceServerFolderPanel.add(new JLabel("Local path server folder"));
		sourceServerFolderPanel.add(sourceServerTempLocalFolderField);
		
		sourceCards = new JPanel(new CardLayout());
		sourceCards.add(sourceFolderPanel, SOURCEFOLDER);
		sourceCards.add(sourceServerFolderPanel, SOURCEVIASERVERFOLDER);
		
		c.gridx = 0;
		c.gridy = 1;
		c.gridwidth = 2;
		mainPanel.add(sourceCards, c);
		sourceCards.setMaximumSize(new Dimension(Integer.MAX_VALUE, sourceCards.getPreferredSize().height));
		
		JPanel etlButtonPanel = new JPanel();
		etlButtonPanel.setLayout(new BoxLayout(etlButtonPanel, BoxLayout.X_AXIS));
		etlButtonPanel.add(Box.createHorizontalGlue());
		
		JButton etl10kButton = new JButton("Perform 10k persons ETL");
		etl10kButton.setBackground(new Color(151, 220, 141));
		etl10kButton.setToolTipText("Perform the ETL for the first 10,000 persons");
		etl10kButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				etlRun(10000);
			}
		});
		componentsToDisableWhenRunning.add(etl10kButton);
		etlButtonPanel.add(etl10kButton);
		
		panel.add(mainPanel, BorderLayout.NORTH);
		
		return panel;
	}
	
	
	private void updateETLType() {
		if ((targetType != null) && (etlType != null)) {
			String currentETLType = (String) etlType.getSelectedItem();
			etlTypeModel.removeAllElements();
			etlTypeModel.addElement(ETLTYPE_LOAD);
			boolean bulkLoadPossible = false;
			if (BULK_LOAD_DATABASE_TYPES.contains((String) targetType.getSelectedItem())) {
				etlTypeModel.addElement(ETLTYPE_BULK_LOAD);
				bulkLoadPossible = true;
			}
			
			if (currentETLType != null) {
				if ((!bulkLoadPossible) && (currentETLType.equals(ETLTYPE_BULK_LOAD))) {
					etlType.setSelectedItem(ETLTYPE_LOAD);
				}
				else {
					etlType.setSelectedItem(currentETLType);
				}
			}
		}
	}
	
	
	private JPanel createExecutePanel() {
		JPanel panel = new JPanel();
		panel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;
		
		JPanel executeCheckboxPanel = new JPanel();
		executeCheckboxPanel.setBorder(BorderFactory.createTitledBorder("Steps to execute"));
		executeCheckboxPanel.setLayout(new BoxLayout(executeCheckboxPanel, BoxLayout.Y_AXIS));
		
		executeStructureCheckBox = new JCheckBox("Create CDM Structure");
		executeCheckboxPanel.add(executeStructureCheckBox);
		executeVocabCheckBox = new JCheckBox("Insert vocabulary");
		executeCheckboxPanel.add(executeVocabCheckBox);
		executeETLCheckBox = new JCheckBox("Perform ETL");
		executeCheckboxPanel.add(executeETLCheckBox);
		executeIndicesCheckBox = new JCheckBox("Create CDM indices");
		executeCheckboxPanel.add(executeIndicesCheckBox);
		executeConstraintsCheckBox = new JCheckBox("Create CDM constraints");
		executeCheckboxPanel.add(executeConstraintsCheckBox);
		executeResultsStructureCheckBox = new JCheckBox("Create Results Structure");
		executeCheckboxPanel.add(executeResultsStructureCheckBox);
		executeConditionErasCheckBox = new JCheckBox("Create condition eras");
		executeCheckboxPanel.add(executeConditionErasCheckBox);
		executeDrugErasCheckBox = new JCheckBox("Create drug eras");
		executeCheckboxPanel.add(executeDrugErasCheckBox);
		executeResultsDataCheckBox = new JCheckBox("Load Results Data");
		executeCheckboxPanel.add(executeResultsDataCheckBox);
		executeResultsIndicesCheckBox = new JCheckBox("Create Results indices");
		executeCheckboxPanel.add(executeResultsIndicesCheckBox);
		c.gridx = 0;
		c.gridy = 0;
		panel.add(executeCheckboxPanel, c);
		
		JPanel optionsCheckboxPanel = new JPanel();
		optionsCheckboxPanel.setBorder(BorderFactory.createTitledBorder("Execute options"));
		optionsCheckboxPanel.setLayout(new BoxLayout(optionsCheckboxPanel, BoxLayout.Y_AXIS));
		
		continueOnErrorCheckBox = new JCheckBox("Continue on ETL, indices, and constraints errors");
		optionsCheckboxPanel.add(continueOnErrorCheckBox);
		optionsCheckboxPanel.add(Box.createVerticalGlue());
		c.gridx = 1;
		c.gridy = 0;
		panel.add(optionsCheckboxPanel, c);
		
		
		JPanel buttonPanel = new JPanel();
		buttonPanel.setLayout(new BoxLayout(buttonPanel, BoxLayout.X_AXIS));
		buttonPanel.add(Box.createHorizontalGlue());
		
		JButton executeButton = new JButton("Execute");
		buttonPanel.add(executeButton);
		executeButton.setBackground(new Color(151, 220, 141));
		executeButton.setToolTipText("Execute all selected steps");
		executeButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (checkInputs()) {
					executeCdmStructureWhenReady = executeStructureCheckBox.isSelected();
					executeVocabWhenReady = executeVocabCheckBox.isSelected();
					executeEtlWhenReady = executeETLCheckBox.isSelected();
					continueOnError = continueOnErrorCheckBox.isSelected();
					executeIndicesWhenReady = executeIndicesCheckBox.isSelected();
					executeConstraintsWhenReady = executeConstraintsCheckBox.isSelected();
					executeConditionErasWhenReady = executeConditionErasCheckBox.isSelected();
					executeDrugErasWhenReady = executeDrugErasCheckBox.isSelected();
					executeResultsStructureWhenReady = executeResultsStructureCheckBox.isSelected();
					executeResultsDataWhenReady = executeResultsDataCheckBox.isSelected();
					executeResultsIndicesWhenReady = executeResultsIndicesCheckBox.isSelected();
					if (	executeCdmStructureWhenReady || 
							executeVocabWhenReady || 
							executeEtlWhenReady || 
							executeIndicesWhenReady || 
							executeConstraintsWhenReady || 
							executeConditionErasWhenReady || 
							executeDrugErasWhenReady || 
							executeResultsStructureWhenReady || 
							executeResultsDataWhenReady || 
							executeResultsIndicesWhenReady) {
						if (
								(!etlType.getSelectedItem().equals("2. PostgreSQL only: Bulk Load CSV files from server in CDM format to server")) ||
								(targetType.getSelectedItem().equals("PostgreSQL")) ||
								(targetType.getSelectedItem().equals("SQL Server"))) {
							runAll();
						}
						else {
							JOptionPane.showMessageDialog(frame, "ETL-Type 2 is only available for PostgreSQL and SQL Server", "Incorrect settings", JOptionPane.ERROR_MESSAGE);
						}
					}
				}
				else {
					
				}
			}
		});
		componentsToDisableWhenRunning.add(executeButton);
		c.gridx = 1;
		c.gridy = 1;
		panel.add(buttonPanel, c);
		
		return panel;
	}
	
	
	private boolean checkInputs() {
		boolean result = true;
		List<String> errors = new ArrayList<String>();
		
		result = folderExists(folderField.getText().trim(), false, "Working folder", errors);
		String connectionResult = testConnectionResult(getTargetDbSettings());
		if (!connectionResult.equals("OK")) {
			errors.add(connectionResult);
			result = false;
		}
		if (executeVocabCheckBox.isSelected()) {
			result = folderExists(vocabFolderField.getText(), false, "Vocabulary data folder", errors);
		}
		if (executeETLCheckBox.isSelected()) {
			result = folderExists(sourceFolderField.getText(), false, "Source folder", errors);
		}
		if (!result) {
			String message = "";
			for (String error : errors) {
				message += (!message.equals("") ? "\r\n" : "") + error;
			}
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Incorrect settings", JOptionPane.ERROR_MESSAGE);
		}
		
		return result;
	}
	
	
	private boolean folderExists(String folderName, boolean mayBeEmpty, String description, List<String> errors) {
		boolean exists = true;
		
		if (folderName.equals("")) {
			if (!mayBeEmpty) {
				errors.add(description + " is not specified");
				exists = false;
			} 
		}
		else {
			File folder = new File(folderName);
			if (!folder.isDirectory()) {
				errors.add(description + " does not exist");
				exists = false;
			}
		}
		
		return exists;
	}
	
	
	private JComponent createConsolePanel() {
		JTextArea consoleArea = new JTextArea();
		consoleArea.setToolTipText("General progress information");
		consoleArea.setEditable(false);
		Console console = new Console();
		console.setTextArea(consoleArea);
		// console.setDebugFile("c:/temp/debug.txt");
		System.setOut(new PrintStream(console));
		System.setErr(new PrintStream(console));
		JScrollPane consoleScrollPane = new JScrollPane(consoleArea);
		consoleScrollPane.setBorder(BorderFactory.createTitledBorder("Console"));
		consoleScrollPane.setPreferredSize(new Dimension(800, 200));
		consoleScrollPane.setAutoscrolls(true);
		ObjectExchange.console = console;
		return consoleScrollPane;
	}
	
	
	private void printUsage() {
		System.out.println("Usage: java -jar JCDMBuilder.jar [options]");
		System.out.println("");
		System.out.println("Options are:");
		System.out.println("");
		System.out.println("-settingsfile <file>        Use the specified settings file");
		System.out.println("-targetpassword <password>  Set target database password");
		System.out.println("");
		System.out.println("When one of the options below is added they overrule the value in");
		System.out.println("the settings file.");
		System.out.println("");
		System.out.println("or:");
		System.out.println("");
		System.out.println("-folder <folder>                   Set working folder");
		System.out.println("-targettype <type>                 Set target type, e.g.");
		System.out.println("                                     '-targettype \"SQL Server\"'");
		System.out.println("-targetserver <server>             Set target server, e.g.");
		System.out.println("                                     '-targetserver myserver.mycompany.com'");
		System.out.println("-targetdatabase <database>         Set target database, e.g.");
		System.out.println("                                     '-targetdatabase cdm_hcup'");
		System.out.println("-targetuser <user>                 Set target database user");
		System.out.println("-targetpassword <password>         Set target database password");
		System.out.println("-vocabsourcetype <type>            Set vocab source type, e.g.");
		System.out.println("                                     '-vocabsourcetype files' or");
		System.out.println("                                     '-vocabsourcetype schema'");
		System.out.println("-vocabfolder <folder>              Set vocab folder when using file type");
		System.out.println("-vocabSchemaField <schema>         Set vocab schema name (on target server)");
		System.out.println("-etlnumber <number>                Set ETL number, e.g.");
		System.out.println("                                     '-etlnumber 4' for HCUP ETL to CDM");
		System.out.println("                                     version 5'");
		System.out.println("-versionid <id>                    Set version ID (integer) to be loaded into");
		System.out.println("                                   _version table");
		System.out.println("-sourcetype <type>                 Set source type, e.g.");
		System.out.println("                                     '-sourcetype \"SQL Server\"'");
		System.out.println("-sourceserver <server>             Set source server, e.g.");
		System.out.println("                                     '-sourceserver myserver.mycompany.com'");
		System.out.println("-sourcedatabase <database>         Set source database, e.g.");
		System.out.println("                                     '-sourcedatabase [hcup-nis]'");
		System.out.println("-sourceuser <user>                 Set source database user");
		System.out.println("-sourcepassword <password>         Set source database password");
		System.out.println("-sourcefolder <folder>             Set source folder containing CSV files");
		System.out.println("-sourceserverfolder <folder>       PostgreSQL only: Set temporary server folder");
		System.out.println("-sourceserverlocalfolder <folder>  PostgreSQL only: Set local path on the");
		System.out.println("                                   server for the temporary server folder");
		System.out.println("-idtobigint  				       When creating the CDM structure, use BIGINT");
		System.out.println("                                   instead of INT for all IDs");
		System.out.println("");
		System.out.println("The following options allow the steps to be automatically executed. Steps are");
		System.out.println("executed in order:");
		System.out.println("");
		System.out.println("-executecdmstructure               Create default CDM structure on startup");
		System.out.println("-executevocab                      Insert vocabulary on startup");
		System.out.println("-executeetl                        Execute ETL on startup");
		System.out.println("-executeindices                    Create required indices on startup");
		System.out.println("-executeconstraints                Add constraints on startup");
		System.out.println("-executeconditioneras              Create condition eras on startup");
		System.out.println("-executedrugeras                   Create drug eras on startup");
		System.out.println("-executeresultsstructure           Create results structure on startup");
		System.out.println("-executeresultsdata                Load results data on startup");
		System.out.println("-executeresultsindices             Create results indices on startup");
		System.out.println("-continueonerror                   Continue after error during ETL, creating");
		System.out.println("                                   indices, and creating constraints.");
	}
	
	
	private void executeParameters(String[] args) {
		String parameter = null;
		String parameterValue = null;
		int argNr = 0;
		while (argNr < args.length) {
			parameter = args[argNr].toLowerCase();
			if (parameter.startsWith("-")) {
				if (parameter.equals("-settingsfile")) {
					argNr++;
					parameterValue = args[argNr];
					loadSettings(parameterValue);
					break;
				}
			}
			parameter = null;
			parameterValue = null;
			argNr++;
		}
		argNr = 0;
		while (argNr < args.length) {
			parameter = args[argNr].toLowerCase();
			if (parameter.startsWith("-")) {
				if (parameter.equals("-folder")) {
					folderField.setText(parameterValue);
				}
				if (parameter.equals("-targetpassword")) {
					argNr++;
					parameterValue = args[argNr];
					targetPasswordField.setText(parameterValue);
				}
				if (parameter.equals("-targetserver")) {
					argNr++;
					parameterValue = args[argNr];
					targetServerField.setText(parameterValue);
				}
				if (parameter.equals("-targettype")) {
					argNr++;
					parameterValue = args[argNr];
					targetType.setSelectedItem(parameterValue);
				}
				if (parameter.equals("-targetdatabase")) {
					argNr++;
					parameterValue = args[argNr];
					targetSchemaField.setText(parameterValue);
				}
				if (parameter.equals("-targetuser")) {
					argNr++;
					parameterValue = args[argNr];
					targetUserField.setText(parameterValue);
				}
				if (parameter.equals("-vocabsourcetype")) {
					argNr++;
					parameterValue = args[argNr];
					int vocabSourceTypNumber = Integer.parseInt(parameterValue);
					vocabSourceType.setSelectedIndex(vocabSourceTypNumber - 1);
				}
				if (parameter.equals("-vocabfolder")) {
					argNr++;
					parameterValue = args[argNr];
					vocabFolderField.setText(parameterValue);
				}
				if (parameter.equals("-vocabschema")) {
					argNr++;
					parameterValue = args[argNr];
					vocabSchemaField.setText(parameterValue);
				}
				if (parameter.equals("-etltype")) {
					argNr++;
					parameterValue = args[argNr];
					int etlNumber = Integer.parseInt(parameterValue);
					etlType.setSelectedIndex(etlNumber - 1);
				}
				if (parameter.equals("-versionid")) {
					argNr++;
					parameterValue = args[argNr];
					versionIdField.setText(parameterValue);
				}
				if (parameter.equals("-sourcefolder")) {
					argNr++;
					parameterValue = args[argNr];
					sourceFolderField.setText(parameterValue);
					sourceServerFolderField.setText(parameterValue);
				}
				if (parameter.equals("-sourceserverfolder")) {
					argNr++;
					parameterValue = args[argNr];
					sourceServerTempFolderField.setText(parameterValue);
				}
				if (parameter.equals("-sourceserverlocalfolder")) {
					argNr++;
					parameterValue = args[argNr];
					sourceServerTempLocalFolderField.setText(parameterValue);
				}
			}
			parameter = null;
			parameterValue = null;
			argNr++;
		}
		for (String arg : args) {
			if (arg.startsWith("-")) {
				parameter = arg.toLowerCase();
				if (parameter.equals("-executecdmstructure"))
					executeCdmStructureWhenReady = true;
				if (parameter.equals("-executevocab"))
					executeVocabWhenReady = true;
				if (parameter.equals("-executeetl"))
					executeEtlWhenReady = true;
				if (parameter.equals("-executeindices"))
					executeIndicesWhenReady = true;
				if (parameter.equals("-executeconstraints"))
					executeConstraintsWhenReady = true;
				if (parameter.equals("-executedrugeras"))
					executeDrugErasWhenReady = true;
				if (parameter.equals("-executeconditioneras"))
					executeConditionErasWhenReady = true;
				if (parameter.equals("-executeresultsstructure"))
					executeResultsStructureWhenReady = true;
				if (parameter.equals("-executeresultsdata"))
					executeResultsDataWhenReady = true;
				if (parameter.equals("-executeresultsindices"))
					executeResultsIndicesWhenReady = true;
				if (parameter.equals("-continueonerror"))
					continueOnError = true;
				if (parameter.equals("-idstobigint")) {
					idsToBigInt = true;
					System.out.println("IDs will be converted to BIGINT");
				}
			}
		}
	}
	
	
	private void pickFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(folderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			folderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}
	
	
	private void loadSettingsFile() {
		JFileChooser fileChooser = new JFileChooser();
		if (settingsFile == null)
			fileChooser.setSelectedFile(new File(System.getProperty("user.dir")));
		else
			fileChooser.setSelectedFile(new File(settingsFile.getFileName()));
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Load");
		if (returnVal == JFileChooser.APPROVE_OPTION) {
			loadSettings(fileChooser.getSelectedFile().getAbsolutePath());
		}
	}
	
	
	private void saveSettingsFile() {
		JFileChooser fileChooser = new JFileChooser();
		if (settingsFile == null)
			fileChooser.setSelectedFile(new File(System.getProperty("user.dir")));
		else
			fileChooser.setSelectedFile(new File(settingsFile.getFileName()));
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Save");
		if (returnVal == JFileChooser.APPROVE_OPTION) {
			saveSettings(fileChooser.getSelectedFile().getAbsolutePath());
		}
	}
	
	
	private void pickVocabFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(vocabFolderField.getText().trim().equals("") ? folderField.getText() : vocabFolderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select vocabulary folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			vocabFolderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}
	
	
	private void pickSourceFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(sourceFolderField.getText().trim().equals("") ? folderField.getText() : sourceFolderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select source folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			sourceFolderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}
	
	
	private void pickTemporaryServerFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(sourceServerTempFolderField.getText().trim().equals("") ? folderField.getText() : sourceServerTempFolderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select temporary server folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			sourceServerTempFolderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}
	
	
	private void testConnection(DbSettings dbSettings, boolean testConnectionToDb) {
		RichConnection connection;
		try {
			connection = new RichConnection(dbSettings.server, dbSettings.domain, dbSettings.user, dbSettings.password, dbSettings.dbType);
		} catch (Exception e) {
			String message = "Could not connect to source server: " + e.getMessage();
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
			return;
		}
		
		if (testConnectionToDb)
			try {
				connection.getTableNames(dbSettings.cdmSchema);
			} catch (Exception e) {
				String message = "Could not connect to database: " + e.getMessage();
				JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error connecting to server", JOptionPane.ERROR_MESSAGE);
				return;
			}
		
		connection.close();
	}
	
	
	private DbSettings getTargetDbSettings() {
		DbSettings dbSettings = new DbSettings();
		dbSettings.dataType = DbSettings.DATABASE;
		dbSettings.user = targetUserField.getText();
		dbSettings.password = targetPasswordField.getText();
		dbSettings.server = targetServerField.getText();
		dbSettings.cdmSchema = targetSchemaField.getText();
		dbSettings.resultsSchema = targetResultsSchemaField.getText();
		if (targetType.getSelectedItem().toString().equals("MySQL"))
			dbSettings.dbType = DbType.MYSQL;
		else if (targetType.getSelectedItem().toString().equals("Oracle"))
			dbSettings.dbType = DbType.ORACLE;
		else if (targetType.getSelectedItem().toString().equals("PostgreSQL"))
			dbSettings.dbType = DbType.POSTGRESQL;
		else if (targetType.getSelectedItem().toString().equals("SQL Server")) {
			dbSettings.dbType = DbType.MSSQL;
			if (targetUserField.getText().length() != 0) { // Not using windows authentication
				String[] parts = targetUserField.getText().split("/");
				if (parts.length < 2) {
					dbSettings.user = targetUserField.getText();
					dbSettings.domain = null;
				} else {
					dbSettings.user = parts[1];
					dbSettings.domain = parts[0];
				}
			}
		}
		
		if (dbSettings.cdmSchema.trim().length() == 0) {
			String message = "Please specify a name for the target database";
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Database error", JOptionPane.ERROR_MESSAGE);
			return null;
		}
		
		return dbSettings;
	}
	
	
	private void etlRun(int maxPersons) {
		EtlThread etlThread = new EtlThread(Cdm.CDM, maxPersons);
		etlThread.start();
	}
	
	
	private class AutoRunThread extends Thread {
		public void run() {
			System.out.println("JCDMBuider Version " + JCdmBuilderMain.VERSION);
			System.out.println();
			
			if (executeResultsStructureWhenReady) {
				DropStructureThread dropStructureThread = new DropStructureThread(Cdm.RESULTS);
				dropStructureThread.run();
			}
			if (executeCdmStructureWhenReady) {
				DropStructureThread dropStructureThread = new DropStructureThread(Cdm.CDM);
				dropStructureThread.run();
			}
			if (executeCdmStructureWhenReady) {
				StructureThread structureThread = new StructureThread(Cdm.CDM);
				structureThread.run();
			}
			if (executeVocabWhenReady) {
				VocabRunThread vocabRunThread = new VocabRunThread();
				vocabRunThread.run();
			}
			if (executeEtlWhenReady) {
				EtlThread etlThread = new EtlThread(Cdm.CDM, Integer.MAX_VALUE);
				etlThread.run();
			}
			if (executeIndicesWhenReady) {
				IndexThread indexThread = new IndexThread(Cdm.CDM);
				indexThread.run();
			}
			if (executeConstraintsWhenReady) {
				ConstraintThread constraintThread = new ConstraintThread(Cdm.CDM);
				constraintThread.run();
			}
			if (executeResultsStructureWhenReady) {
				StructureThread structureThread = new StructureThread(Cdm.RESULTS);
				structureThread.run();
			}
			if (executeConditionErasWhenReady) {
				EraThread eraThread = new EraThread(EraThread.CONDITIONS);
				eraThread.run();
			}
			if (executeDrugErasWhenReady) {
				EraThread eraThread = new EraThread(EraThread.DRUGS);
				eraThread.run();
			}
			if (executeResultsDataWhenReady) {
				EtlThread etlThread = new EtlThread(Cdm.RESULTS, Integer.MAX_VALUE);
				etlThread.run();
			}
			if (executeResultsIndicesWhenReady) {
				IndexThread indexThread = new IndexThread(Cdm.RESULTS);
				indexThread.run();
			}
			if (continueOnError) {
				if (errors.size() > 0) { // Show error overview
					String errorMessage = "The following tables had errors:\n";
					for (String table : errors) {
						errorMessage += "\n    " + table;
					}
					JOptionPane.showMessageDialog(frame, errorMessage, "Error", JOptionPane.ERROR_MESSAGE);
				}
				else if (autoStart) {
					System.exit(0);
				}
			}
		}
	}
	
	
	private class EtlThread extends Thread {

		private int structure;
		private int maxPersons;
		
		public EtlThread(int structure, int maxPersons) {
			this.structure = structure;
			this.maxPersons = maxPersons;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			
			try {
				if (etlType.getSelectedItem().equals("1. Load CSV files in CDM format to server") || (structure == Cdm.RESULTS)) {
					CdmEtl etl = new CdmEtl();
					DbSettings dbSettings = getTargetDbSettings();
					testConnection(dbSettings, false);
					if (dbSettings != null)
						etl.process(structure, sourceFolderField.getText(), sourceDelimiterField.getText(), sourceQuoteField.getText(), sourceNullValueField.getText(), dbSettings, maxPersons, Integer.parseInt(versionIdField.getText()), targetCdmVersion.getSelectedItem().toString(), frame, folderField.getText(), continueOnError);
				}
				if (etlType.getSelectedItem().equals("2. Bulk Load CSV files from server in CDM format to server")) {
					CdmEtl etl = new CdmEtl();
					DbSettings dbSettings = getTargetDbSettings();
					testConnection(dbSettings, false);
					if (dbSettings != null)
						etl.process(structure, sourceServerFolderField.getText(), sourceServerDelimiterField.getText(), sourceServerQuoteField.getText(), sourceServerNullValueField.getText(), sourceServerTempFolderField.getText(), sourceServerTempLocalFolderField.getText(), dbSettings, maxPersons, Integer.parseInt(versionIdField.getText()), targetCdmVersion.getSelectedItem().toString(), frame, folderField.getText(), continueOnError);
				}
				
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
				
			}
		}
		
	}
	
	
	private class VocabRunThread extends Thread {
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				if (vocabSourceType.getSelectedItem().toString().equals(VOCABTYPE_LOAD)) {
					InsertVocabularyInServer process = new InsertVocabularyInServer();
					DbSettings dbSettings = getTargetDbSettings();
					if (dbSettings != null)
						process.process(vocabFolderField.getText(), dbSettings);
				}
				else if (vocabSourceType.getSelectedItem().toString().equals(VOCABTYPE_BULK_LOAD)) {
					InsertVocabularyInServer process = new InsertVocabularyInServer();
					DbSettings dbSettings = getTargetDbSettings();
					if (dbSettings != null)
						process.process(vocabFolderField.getText(), vocabServerTempFolderField.getText(), vocabServerTempLocalFolderField.getText(), dbSettings, frame, folderField.getText());
				}
				else if (vocabSourceType.getSelectedItem().toString().equals(VOCABTYPE_SCHEMA_LOAD)) {
					CopyVocabularyFromSchema process = new CopyVocabularyFromSchema();
					DbSettings dbSettings = getTargetDbSettings();
					if (dbSettings != null)
						process.process(vocabSchemaField.getText(), dbSettings);
				}
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
			
		}
	}
	
	
	private class DropStructureThread extends Thread {
		private int structure;
		
		public DropStructureThread(int structure) {
			this.structure = structure;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getTargetDbSettings();
				String version = targetCdmVersion.getSelectedItem().toString();
				Cdm.dropStructure(structure, dbSettings, version, sourceFolderField.getText());
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}
	
	
	private class StructureThread extends Thread {
		private int structure;
		
		public StructureThread(int structure) {
			this.structure = structure;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getTargetDbSettings();
				String version = targetCdmVersion.getSelectedItem().toString();
				Cdm.createSchema(structure, dbSettings, version);
				Cdm.createTables(structure, dbSettings, version, sourceFolderField.getText(), idsToBigInt);
				Cdm.createPatchTables(structure, dbSettings, version, sourceFolderField.getText(), idsToBigInt);
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}
	
	
	private class IndexThread extends Thread {
		private int structure;
		
		public IndexThread(int structure) {
			this.structure = structure;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getTargetDbSettings();
				String version = targetCdmVersion.getSelectedItem().toString();
				Cdm.createIndices(structure, dbSettings, version, sourceFolderField.getText());
				Cdm.createPatchIndices(structure, dbSettings, version, sourceFolderField.getText());
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}
	
	
	private class ConstraintThread extends Thread {
		private int structure;
		
		public ConstraintThread(int structure) {
			this.structure = structure;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getTargetDbSettings();
				String version = targetCdmVersion.getSelectedItem().toString();
				Cdm.createConstraints(structure, dbSettings, version, sourceFolderField.getText());
				Cdm.createPatchConstraints(structure, dbSettings, version, version);
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}
	
	
	private class EraThread extends Thread {
		
		private int				type;
		
		public final static int	DRUGS		= 1;
		public final static int	CONDITIONS	= 2;
		
		public EraThread(int type) {
			this.type = type;
		}
		
		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getTargetDbSettings();
				int version = EraBuilder.VERSION_5;
				switch (targetCdmVersion.getSelectedItem().toString()) {
				case "5.0.1":
				case "5.3.0":
				case "5.3.1":
					version = EraBuilder.VERSION_5;
					break;
				case "6.0.0":
					version = EraBuilder.VERSION_6;
					break;
				default:
					break;
				}
				int domain = type == DRUGS ? EraBuilder.DRUG_ERA : EraBuilder.CONDITION_ERA;
				EraBuilder.buildEra(dbSettings, version, sourceFolderField.getText(), domain);
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}
	
	
	private class TextFieldLink implements DocumentListener {
		private JTextField sourceField;
		private JTextField linkedField;
		
		public TextFieldLink(JTextField sourceField, JTextField linkedField) {
			this.sourceField = sourceField;
			this.linkedField = linkedField;
		}

		@Override
		public void insertUpdate(DocumentEvent e) {
			copyValueToLinkedField();
		}

		@Override
		public void removeUpdate(DocumentEvent e) {
			copyValueToLinkedField();
		}

		@Override
		public void changedUpdate(DocumentEvent e) {
			copyValueToLinkedField();
		} 
		
		private void copyValueToLinkedField() {
			if (!fieldLinkActive) {
				fieldLinkActive = true;
				linkedField.setText(sourceField.getText());
				fieldLinkActive = false;
			}
		}
	}
	
	
	private void handleError(Exception e) {
		if (!e.getMessage().equals("NO ERROR")) {
			System.err.println("Error: " + e.getMessage());
			String errorReportFilename = ErrorReport.generate(folderField.getText(), e, null);
			String message = "Error: " + e.getLocalizedMessage();
			message += "\nAn error report has been generated:\n" + errorReportFilename;
			System.out.println(message);
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error", JOptionPane.ERROR_MESSAGE);
		}
	}
	
	
	private void runAll() {
		ObjectExchange.console.setDebugFile(folderField.getText() + "/Console.txt");
		AutoRunThread autoRunThread = new AutoRunThread();
		autoRunThread.start();
	}
}
