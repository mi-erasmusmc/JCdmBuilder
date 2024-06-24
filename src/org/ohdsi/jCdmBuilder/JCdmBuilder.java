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
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.DefaultComboBoxModel;
import javax.swing.GroupLayout;
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
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.DocumentFilter;
import javax.swing.text.PlainDocument;

import org.ohdsi.databases.DbType;
import org.ohdsi.databases.RichConnection;
import org.ohdsi.jCdmBuilder.cdm.Cdm;
import org.ohdsi.jCdmBuilder.cdm.EraBuilder;
import org.ohdsi.jCdmBuilder.etls.cdm.CdmEtl;
import org.ohdsi.jCdmBuilder.vocabulary.CopyVocabularyFromSchema;
import org.ohdsi.jCdmBuilder.vocabulary.InsertVocabularyInServer;
import org.ohdsi.utilities.StringUtilities;
import org.ohdsi.utilities.files.IniFile;

public class JCdmBuilder {
	public static final String VERSION = "5.4.1.7";

	private static final String ICON = "/org/ohdsi/jCdmBuilder/OHDSI Icon Picture 048x048.gif";

	private static String					DATABASE_TYPE_POSTGRESQL				= "PostgreSQL";
	private static String					DATABASE_TYPE_ORACLE					= "Oracle";
	private static String					DATABASE_TYPE_SQLSERVER					= "SQL Server";

	private static String[]					DATABASE_TYPES							= new String[] { DATABASE_TYPE_POSTGRESQL, DATABASE_TYPE_ORACLE, DATABASE_TYPE_SQLSERVER };
	private static HashSet<String>			BULK_LOAD_DATABASE_TYPES				= new HashSet<String>() {
																						private static final long serialVersionUID = 946936162824903605L;
																						{
																							add(DATABASE_TYPE_POSTGRESQL);
																							add(DATABASE_TYPE_SQLSERVER);
																						}};
	private static HashSet<String>			SCHEMA_LOAD_DATABASE_TYPES				= new HashSet<String>() {
																						private static final long serialVersionUID = 2059669382590652555L;
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
	private JTextField						workingFolderField;
	private JTextField						localScriptsFolderField;
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
	private JCheckBox						executePrimaryKeysCheckBox;
	private JCheckBox						executeIndicesCheckBox;
	private JCheckBox						executeConstraintsCheckBox;
	private JCheckBox						executeConditionErasCheckBox;
	private JCheckBox						executeDrugErasCheckBox;
	private JCheckBox						executeResultsStructureCheckBox;
	private JComboBox<String>				etlType;
	private DefaultComboBoxModel<String>	etlTypeModel;
	private JComboBox<String>				targetType;
	private JTextField						versionIdField;
	private JTextField						targetUserField;
	private JTextField						targetPasswordField;
	private JTextField						targetServerField;
	private JTextField						targetSchemaField;
	private JTextField						targetResultsSchemaField;
	private JTextField                      targetTempSchemaField;
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
	private JTextField                      webAPIServerField;
	private JTextField                      webAPIPortField;
	private boolean							executeCdmStructureWhenReady		    = false;
	private boolean							executeVocabWhenReady				    = false;
	private boolean							executeEtlWhenReady					    = false;
	private boolean							executePrimaryKeysWhenReady				= false;
	private boolean							executeIndicesWhenReady				    = false;
	private boolean							executeConstraintsWhenReady			    = false;
	private boolean							executeConditionErasWhenReady		    = false;
	private boolean							executeDrugErasWhenReady			    = false;
	private boolean							executeResultsStructureWhenReady	    = false;
	private boolean							idsToBigInt							    = false;
	private boolean             			continueOnError                         = false;
	private IniFile							settingsFile							= null;

	private boolean							fieldLinkActive							= false;
	private List<JComponent>				componentsToDisableWhenRunning	        = new ArrayList<JComponent>();
	private boolean             			autoStart                               = false;


	public static void main(String[] args) {
		JCdmBuilder.errors = new ArrayList<String>();
		new JCdmBuilder(args);
	}


	/**
	 * Sets an icon on a JFrame or a JDialog.
	 * @param container - the GUI component on which the icon is to be put
	 */
	public static void setIcon(Object container){
		URL url = JCdmBuilder.class.getResource(ICON);
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


	public JCdmBuilder(String[] args) {
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
		JCdmBuilder.setIcon(frame);

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
				executePrimaryKeysWhenReady ||
				executeIndicesWhenReady ||
				executeConstraintsWhenReady ||
				executeConditionErasWhenReady ||
				executeDrugErasWhenReady ||
				executeConditionErasWhenReady ||
				executeResultsStructureWhenReady) {
			autoStart = true;
			ObjectExchange.console.setDebugFile(workingFolderField.getText() + "/Console.txt");
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

		JPanel webAPIPanel = createWebAPIPanel();
		tabbedPane.addTab("WebAPI", null, webAPIPanel, "WebAPI Defintion");

		JPanel executePanel = createExecutePanel();
		tabbedPane.addTab("Execute", null, executePanel, "Run multiple steps automatically");

		tabbedPane.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent changeEvent) {
				JTabbedPane sourceTabbedPane = (JTabbedPane) changeEvent.getSource();
				int index = sourceTabbedPane.getSelectedIndex();
				if (sourceTabbedPane.getTitleAt(index).equals("WebAPI")) {
					if (webAPIServerField.getText().equals("") && (!targetServerField.getText().equals(""))) {
						webAPIServerField.setText(DbSettings.getServerNameFromServer(targetServerField.getText()));
					}
					if (webAPIPortField.getText().equals("")) {
						webAPIPortField.setText("8080");
					}
				}
			}
		});

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
		GroupLayout folderPanelLayout = new GroupLayout(folderPanel);
		folderPanelLayout.setAutoCreateGaps(true);
		folderPanel.setLayout(folderPanelLayout);
		folderPanel.setBorder(BorderFactory.createTitledBorder("Folders"));
		
		JLabel workingFolderLabel = new JLabel("Working Folder:");
		workingFolderField = new JTextField();
		workingFolderField.setText("");
		workingFolderField.setToolTipText("The folder where all output will be written");
		JButton workingFolderPickButton = new JButton("Select");
		workingFolderPickButton.setToolTipText("Pick a different working folder");
		workingFolderPickButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickFolder(workingFolderField);
			}
		});
		componentsToDisableWhenRunning.add(workingFolderPickButton);
		
		JLabel localScriptsFolderLabel = new JLabel("Local Scripts Folder:");
		localScriptsFolderField = new JTextField();
		localScriptsFolderField.setText("");
		localScriptsFolderField.setToolTipText("The folder where local SQL scripts are");
		JButton localScriptsFolderPickButton = new JButton("Select");
		localScriptsFolderPickButton.setToolTipText("Pick a different scripts folder");
		localScriptsFolderPickButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				pickFolder(localScriptsFolderField);
			}
		});
		componentsToDisableWhenRunning.add(localScriptsFolderPickButton);
		
		folderPanelLayout.setHorizontalGroup(folderPanelLayout.createSequentialGroup()
				.addGroup(folderPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addComponent(workingFolderLabel)
						.addComponent(localScriptsFolderLabel)
						)
				.addGroup(folderPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addComponent(workingFolderField)
						.addComponent(localScriptsFolderField)
						)
				.addGroup(folderPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addComponent(workingFolderPickButton)
						.addComponent(localScriptsFolderPickButton)
						)
				);
		folderPanelLayout.setVerticalGroup(folderPanelLayout.createSequentialGroup()
				.addGroup(folderPanelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
						.addComponent(workingFolderLabel)
						.addComponent(workingFolderField)
						.addComponent(workingFolderPickButton)
						)
				.addGroup(folderPanelLayout.createParallelGroup(GroupLayout.Alignment.BASELINE)
						.addComponent(localScriptsFolderLabel)
						.addComponent(localScriptsFolderField)
						.addComponent(localScriptsFolderPickButton)
						)
				);
		folderPanelLayout.linkSize(SwingConstants.HORIZONTAL, workingFolderLabel, localScriptsFolderLabel);
		folderPanelLayout.linkSize(SwingConstants.HORIZONTAL, workingFolderPickButton, localScriptsFolderPickButton);
		folderPanelLayout.linkSize(SwingConstants.VERTICAL, workingFolderField, workingFolderPickButton);
		folderPanelLayout.linkSize(SwingConstants.VERTICAL, localScriptsFolderField, localScriptsFolderPickButton);
		
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
		targetPanel.add(new JLabel("Temp schema"));
		targetTempSchemaField = new JTextField();
		targetTempSchemaField.setText("");
		targetTempSchemaField.setToolTipText("Specify the WebAPI temp schema.");
		targetPanel.add(targetTempSchemaField);
		targetPanel.add(new JLabel("CDM version"));
		targetCdmVersion = new JComboBox<String>(getAvailableVersions());
		targetCdmVersion.setToolTipText("Select the CMD version");
		targetCdmVersion.setSelectedItem(VERSION.substring(0, VERSION.lastIndexOf('.')));

		targetType.addItemListener(new ItemListener() {

			@Override
			public void itemStateChanged(ItemEvent arg0) {
				updateETLType();
				updateVocabSourceType();

				if (arg0.getItem().toString().equals(DATABASE_TYPE_ORACLE)) {
					targetServerField.setToolTipText("For Oracle servers this field contains the SID, servicename, and optionally the port: '<host>/<sid>', '<host>:<port>/<sid>', '<host>/<service name>', or '<host>:<port>/<service name>'.");
					targetUserField.setToolTipText("For Oracle servers this field contains the name of the user used to log in.");
					targetPasswordField.setToolTipText("For Oracle servers this field contains the password corresponding to the user.");
					targetSchemaField.setToolTipText("For Oracle servers this field contains the schema (i.e. 'user' in Oracle terms) containing the target tables. The user will be created with the same password.");
				} else if (arg0.getItem().toString().equals(DATABASE_TYPE_POSTGRESQL)) {
					targetServerField.setToolTipText("For PostgreSQL servers this field contains the host name and database name (<host>/<database>).");
					targetUserField.setToolTipText("The user used to log in to the server.");
					targetPasswordField.setToolTipText("The password used to log in to the server.");
					targetSchemaField.setToolTipText("For PostgreSQL servers this field contains the schema containing the target tables.");
				} else if (arg0.getItem().toString().equals(DATABASE_TYPE_SQLSERVER)) {
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
	
	
	private String[] getAvailableVersions() {
		String[] availableVersions = new String[] {};
		String rootPath = "/org/ohdsi/jCdmBuilder/cdm";
		String nativeRootPath = "";
		for (int pos = 0; pos < rootPath.length(); pos++) {
			nativeRootPath += rootPath.substring(pos, pos + 1).equals("/") ? File.separator : rootPath.substring(pos, pos + 1);
		}
		List<String> versions = new ArrayList<String>();

        URI uri;
		try {
			uri = JCdmBuilder.class.getResource(rootPath).toURI();
	        if (uri.getScheme().equals("file")) {
	        	// Running in IDE
		        Path myPath;
	            myPath = Paths.get(uri);
		        Stream<Path> walk;
				walk = Files.walk(myPath, 1);
				Path versionPath = null;
		        for (Iterator<Path> it = walk.iterator(); it.hasNext();){
		        	Path path = it.next();
		            if (path.toString().contains(nativeRootPath + File.separator + "v")) {
		            	versionPath = path;
		            	break;
		            }
		        }
		        walk.close();
		        if (versionPath != null) {
					walk = Files.walk(versionPath, 1);
			        for (Iterator<Path> it = walk.iterator(); it.hasNext();){
			        	String name = it.next().toString();
	        			if (
	        					name.contains(nativeRootPath + File.separator + "v") &&
	        					name.substring(name.indexOf(nativeRootPath) + nativeRootPath.length() + 2).contains(File.separator) &&
	        					(!name.endsWith(".class"))
	        			) {
	        				versions.add(name.substring(name.lastIndexOf(File.separator) + 1));
	        			}
			        }
			        walk.close();
		        }
	        }
	        else {
	        	// Running as jar
				String jarPath = JCdmBuilder.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
        		URL jar = new URL("file:" + jarPath);
        		rootPath = rootPath.substring(1);
        		ZipInputStream zip = new ZipInputStream(jar.openStream());
        		while(true) {
        			ZipEntry e = zip.getNextEntry();
        			if (e == null)
        				break;
        			String name = e.getName();
        			if (
        					name.startsWith(rootPath + "/v") &&
        					name.substring(rootPath.length() + 2, name.length() - 1).contains("/") &&
        					name.endsWith("/") &&
        					(!name.endsWith(".class"))
        			) {
        				name = name.substring(0, name.length() - 1);
        				versions.add(name.substring(name.lastIndexOf("/") + 1));
        			}
        		}
	        }
		} catch (URISyntaxException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if (versions.size() > 0) {
			availableVersions = new String[versions.size()];
			versions.toArray(availableVersions);
		}
		
		return availableVersions;
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
		if (dbSettings.server == null || dbSettings.server.equals(""))
			result = "Please specify the server";
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

				result = "OK";
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
		if (settingsFile.getValue("Locations", "Workspace Folder")           != null) workingFolderField.setText(settingsFile.getValue("Locations", "Workspace Folder"));
		if (settingsFile.getValue("Locations", "Local Scripts Folder")       != null) localScriptsFolderField.setText(settingsFile.getValue("Locations", "Local Scripts Folder"));
		if (settingsFile.getValue("Locations", "Target Database Type")       != null) targetType.setSelectedItem(settingsFile.getValue("Locations", "Target Database Type"));
		if (settingsFile.getValue("Locations", "Target Server Location")     != null) targetServerField.setText(settingsFile.getValue("Locations", "Target Server Location"));
		if (settingsFile.getValue("Locations", "Target User Name")           != null) targetUserField.setText(settingsFile.getValue("Locations", "Target User Name"));
		if (settingsFile.getValue("Locations", "Target CDM Schema Name")     != null) targetSchemaField.setText(settingsFile.getValue("Locations", "Target CDM Schema Name"));
		if (settingsFile.getValue("Locations", "Target Results Schema Name") != null) targetResultsSchemaField.setText(settingsFile.getValue("Locations", "Target Results Schema Name"));
		if (settingsFile.getValue("Locations", "Target Temp Schema Name")    != null) targetTempSchemaField.setText(settingsFile.getValue("Locations", "Target Temp Schema Name"));
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

		// WebAPI
		if (settingsFile.getValue("WebAPI", "Server")                        != null) webAPIServerField.setText(settingsFile.getValue("WebAPI", "Server"));
		if (settingsFile.getValue("WebAPI", "Port")                          != null) webAPIPortField.setText(settingsFile.getValue("WebAPI", "Port"));
	}


	private void saveSettings(String fileName) {
		settingsFile = new IniFile(fileName);

		// Locations
		settingsFile.addGroup("Locations", null);
		settingsFile.setValue("Locations", "Workspace Folder", workingFolderField.getText(), null);
		settingsFile.setValue("Locations", "Local Scripts Folder", localScriptsFolderField.getText(), null);
		settingsFile.setValue("Locations", "Target Database Type", targetType.getSelectedItem().toString(), null);
		settingsFile.setValue("Locations", "Target Server Location", targetServerField.getText(), null);
		settingsFile.setValue("Locations", "Target User Name", targetUserField.getText(), null);
		settingsFile.setValue("Locations", "Target CDM Schema Name", targetSchemaField.getText(), null);
		settingsFile.setValue("Locations", "Target Results Schema Name", targetResultsSchemaField.getText(), null);
		settingsFile.setValue("Locations", "Target Temp Schema Name",targetTempSchemaField.getText(), null);
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

		// WebAPI
		settingsFile.setValue("WebAPI", "Server", webAPIServerField.getText(), null);
		settingsFile.setValue("WebAPI", "Port", webAPIPortField.getText(), null);

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
			String currentVocabSourceType = (String) vocabSourceType.getSelectedItem();
			vocabSourceTypeModel.removeAllElements();
			vocabSourceTypeModel.addElement(VOCABTYPE_LOAD);
			boolean bulkLoadPossible = false;
			boolean schemaLoadPossible = false;
			if (BULK_LOAD_DATABASE_TYPES.contains((String) targetType.getSelectedItem())) {
				vocabSourceTypeModel.addElement(VOCABTYPE_BULK_LOAD);
				bulkLoadPossible = true;
			}
			if (SCHEMA_LOAD_DATABASE_TYPES.contains((String) targetType.getSelectedItem())) {
				vocabSourceTypeModel.addElement(VOCABTYPE_SCHEMA_LOAD);
				schemaLoadPossible = true;
			}

			if (currentVocabSourceType != null) {
				if (((!bulkLoadPossible) && (currentVocabSourceType.equals(VOCABTYPE_BULK_LOAD))) || ((!schemaLoadPossible) && (currentVocabSourceType.equals(VOCABTYPE_SCHEMA_LOAD)))) {
					vocabSourceType.setSelectedItem(VOCABTYPE_LOAD);
				}
				else {
					vocabSourceType.setSelectedItem(currentVocabSourceType);
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


	private JPanel createWebAPIPanel() {
		JPanel panel = new JPanel(new BorderLayout());

		JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new GridBagLayout());
		GridBagConstraints c = new GridBagConstraints();
		c.fill = GridBagConstraints.BOTH;
		c.weightx = 0.5;

		JPanel webAPIPanel = new JPanel();
		webAPIPanel.setLayout(new GridLayout(0, 2));
		webAPIPanel.setBorder(BorderFactory.createTitledBorder("WebAPI Definition"));

		webAPIPanel.add(new JLabel("Server"));
		webAPIServerField = new JTextField();
		webAPIServerField.setText("");
		webAPIServerField.setToolTipText("Specify the WebAPI server.");
		webAPIPanel.add(webAPIServerField);

		webAPIPanel.add(new JLabel("Port"));
		webAPIPortField = new JTextField();
		webAPIPortField.setText("");
		webAPIPortField.setToolTipText("Specify the server port used by the WebAPI. Default is 8080.");
		((PlainDocument) webAPIPortField.getDocument()).setDocumentFilter(new IntegerFilter());
		webAPIPanel.add(webAPIPortField);

		c.gridx = 0;
		c.gridy = 0;
		c.gridwidth = 1;
		mainPanel.add(webAPIPanel, c);

		panel.add(mainPanel, BorderLayout.NORTH);

		return panel;
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
		executePrimaryKeysCheckBox = new JCheckBox("Create CDM primary keys");
		executeCheckboxPanel.add(executePrimaryKeysCheckBox);
		executeIndicesCheckBox = new JCheckBox("Create CDM indices");
		executeCheckboxPanel.add(executeIndicesCheckBox);
		executeConstraintsCheckBox = new JCheckBox("Create CDM constraints");
		executeCheckboxPanel.add(executeConstraintsCheckBox);
		executeConditionErasCheckBox = new JCheckBox("Create condition eras");
		executeCheckboxPanel.add(executeConditionErasCheckBox);
		executeDrugErasCheckBox = new JCheckBox("Create drug eras");
		executeCheckboxPanel.add(executeDrugErasCheckBox);
		executeResultsStructureCheckBox = new JCheckBox("Create Results Structure");
		executeCheckboxPanel.add(executeResultsStructureCheckBox);
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
					executePrimaryKeysWhenReady = executePrimaryKeysCheckBox.isSelected();
					executeIndicesWhenReady = executeIndicesCheckBox.isSelected();
					executeConstraintsWhenReady = executeConstraintsCheckBox.isSelected();
					executeConditionErasWhenReady = executeConditionErasCheckBox.isSelected();
					executeDrugErasWhenReady = executeDrugErasCheckBox.isSelected();
					executeResultsStructureWhenReady = executeResultsStructureCheckBox.isSelected();
					if (	executeCdmStructureWhenReady ||
							executeVocabWhenReady ||
							executeEtlWhenReady ||
							executePrimaryKeysWhenReady ||
							executeIndicesWhenReady ||
							executeConstraintsWhenReady ||
							executeConditionErasWhenReady ||
							executeDrugErasWhenReady ||
							executeResultsStructureWhenReady) {
						if (
								(!etlType.getSelectedItem().equals(ETLTYPE_BULK_LOAD)) ||
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

		result = folderExists(workingFolderField.getText().trim(), false, "Working folder", errors);
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
		System.out.println("-patchScriptsPath <path>    Set the path for SQL patch scripts.");
		System.out.println("                            If not specified it looks for them");
		System.out.println("                            in the 'SQL Scripts' folder next to");
		System.out.println("                            the builder .jar file.");
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
		System.out.println("-continueonerror                   Continue after error during ETL, creating");
		System.out.println("                                   indices, and creating constraints.");
	}


	private void executeParameters(String[] args) {
		executeCdmStructureWhenReady		    = false;
		executeVocabWhenReady				    = false;
		executeEtlWhenReady					    = false;
		executePrimaryKeysWhenReady				= false;
		executeIndicesWhenReady				    = false;
		executeConstraintsWhenReady			    = false;
		executeConditionErasWhenReady		    = false;
		executeDrugErasWhenReady			    = false;
		executeResultsStructureWhenReady	    = false;
		idsToBigInt							    = false;
		continueOnError                         = false;

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
				}
				if (parameter.equals("-targetpassword")) {
					argNr++;
					parameterValue = args[argNr];
					targetPasswordField.setText(parameterValue);
				}
				if (parameter.equals("-executecdmstructure"))
					executeCdmStructureWhenReady = true;
				if (parameter.equals("-executevocab"))
					executeVocabWhenReady = true;
				if (parameter.equals("-executeetl"))
					executeEtlWhenReady = true;
				if (parameter.equals("-executeprimarykeys"))
					executePrimaryKeysWhenReady = true;
				if (parameter.equals("-executeindices"))
					executeIndicesWhenReady = true;
				if (parameter.equals("-executeconstraints"))
					executeConstraintsWhenReady = true;
				if (parameter.equals("-executeconditioneras"))
					executeConditionErasWhenReady = true;
				if (parameter.equals("-executedrugeras"))
					executeDrugErasWhenReady = true;
				if (parameter.equals("-executeresultsstructure"))
					executeResultsStructureWhenReady = true;
				// NOT USED ANYMORE? if (parameter.equals("-executeresultsdata"))
				// NOT USED ANYMORE? 	executeResultsDataWhenReady = true;
				// NOT USED ANYMORE? if (parameter.equals("-executeresultsindices"))
				// NOT USED ANYMORE? 	executeResultsIndicesWhenReady = true;
				if (parameter.equals("-continueonerror"))
					continueOnError = true;
				if (parameter.equals("-idstobigint")) {
					idsToBigInt = true;
					System.out.println("IDs will be converted to BIGINT");
				}
			}
			parameter = null;
			parameterValue = null;
			argNr++;
		}

		executeStructureCheckBox.setSelected(executeCdmStructureWhenReady);
		executeVocabCheckBox.setSelected(executeVocabWhenReady);
		executeETLCheckBox.setSelected(executeEtlWhenReady);
		executePrimaryKeysCheckBox.setSelected(executePrimaryKeysWhenReady);
		executeIndicesCheckBox.setSelected(executeIndicesWhenReady);
		executeConstraintsCheckBox.setSelected(executeConstraintsWhenReady);
		executeConditionErasCheckBox.setSelected(executeConditionErasWhenReady);
		executeDrugErasCheckBox.setSelected(executeDrugErasWhenReady);
		executeResultsStructureCheckBox.setSelected(executeResultsStructureWhenReady);
		continueOnErrorCheckBox.setSelected(continueOnError);
	}


	private void pickFolder(JTextField folderField) {
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
		JFileChooser fileChooser = new JFileChooser(new File(vocabFolderField.getText().trim().equals("") ? workingFolderField.getText() : vocabFolderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select vocabulary folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			vocabFolderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}


	private void pickSourceFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(sourceFolderField.getText().trim().equals("") ? workingFolderField.getText() : sourceFolderField.getText()));
		fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		int returnVal = fileChooser.showDialog(frame, "Select source folder");
		if (returnVal == JFileChooser.APPROVE_OPTION)
			sourceFolderField.setText(fileChooser.getSelectedFile().getAbsolutePath());
	}


	private void pickTemporaryServerFolder() {
		JFileChooser fileChooser = new JFileChooser(new File(sourceServerTempFolderField.getText().trim().equals("") ? workingFolderField.getText() : sourceServerTempFolderField.getText()));
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


	private class IntegerFilter extends DocumentFilter {

		@Override
		public void insertString(FilterBypass fb, int offset, String string, AttributeSet attr) throws BadLocationException {
			Document doc = webAPIPortField.getDocument();
			StringBuilder sb = new StringBuilder();
			sb.append(doc.getText(0, doc.getLength()));
			sb.insert(offset, string);

			if (test(sb)) {
				super.insertString(fb, offset, string, attr);
			}
		}

		@Override
		public void replace(FilterBypass fb, int offset, int length, String text, AttributeSet attr) throws BadLocationException {
			Document doc = webAPIPortField.getDocument();
			StringBuilder sb = new StringBuilder();
			sb.append(doc.getText(0, doc.getLength()));
			sb.replace(offset, offset + length, text);

			if (sb.toString().equals("") || test(sb)) {
				super.replace(fb, offset, length, text, attr);
			}
		}

		@Override
		public void remove(FilterBypass fb, int offset, int length) throws BadLocationException {
			Document doc = webAPIPortField.getDocument();
			StringBuilder sb = new StringBuilder();
			sb.append(doc.getText(0, doc.getLength()));
			sb.delete(offset, offset + length);

			if (sb.toString().equals("") || test(sb)) {
				super.remove(fb, offset, length);
			}
		}

		private boolean test(StringBuilder sb) {
			try {
				Integer.parseInt(sb.toString());
				return true;
			} catch (NumberFormatException exception) {
				return false;
			}

		}
	}


	private DbSettings getTargetDbSettings() {
		DbSettings dbSettings = new DbSettings();
		dbSettings.dataType = DbSettings.DATABASE;
		dbSettings.user = targetUserField.getText();
		dbSettings.password = targetPasswordField.getText();
		dbSettings.server = targetServerField.getText();
		dbSettings.cdmSchema = targetSchemaField.getText();
		dbSettings.resultsSchema = targetResultsSchemaField.getText();
		dbSettings.tempSchema = targetTempSchemaField.getText();
		dbSettings.dbType = DbType.getDbType(targetType.getSelectedItem().toString());
		if (dbSettings.dbType == DbType.MSSQL) {
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
			System.out.println("JCDMBuider Version " + JCdmBuilder.VERSION);
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
			if (executePrimaryKeysWhenReady) {
				PrimaryKeysThread primaryKeysThread = new PrimaryKeysThread(Cdm.CDM);
				primaryKeysThread.run();
			}
			if (executeIndicesWhenReady) {
				IndexThread indexThread = new IndexThread(Cdm.CDM);
				indexThread.run();
			}
			if (executeConstraintsWhenReady) {
				ConstraintThread constraintThread = new ConstraintThread(Cdm.CDM);
				constraintThread.run();
			}
			if (executeConditionErasWhenReady) {
				EraThread eraThread = new EraThread(EraThread.CONDITIONS);
				eraThread.run();
			}
			if (executeDrugErasWhenReady) {
				EraThread eraThread = new EraThread(EraThread.DRUGS);
				eraThread.run();
			}
			if (executeResultsStructureWhenReady) {
				StructureThread structureThread = new StructureThread(Cdm.RESULTS);
				structureThread.run();
			}

			StringUtilities.outputWithTime("Ready");

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
						etl.process(structure, sourceFolderField.getText(), sourceDelimiterField.getText(), sourceQuoteField.getText(), sourceNullValueField.getText(), dbSettings, maxPersons, Integer.parseInt(versionIdField.getText()), targetCdmVersion.getSelectedItem().toString(), frame, workingFolderField.getText(), continueOnError);
				}
				if (etlType.getSelectedItem().equals("2. Bulk Load CSV files from server in CDM format to server")) {
					CdmEtl etl = new CdmEtl();
					DbSettings dbSettings = getTargetDbSettings();
					testConnection(dbSettings, false);
					if (dbSettings != null)
						etl.process(structure, sourceServerFolderField.getText(), sourceServerDelimiterField.getText(), sourceServerQuoteField.getText(), sourceServerNullValueField.getText(), workingFolderField.getText(), sourceServerTempFolderField.getText(), sourceServerTempLocalFolderField.getText(), dbSettings, maxPersons, Integer.parseInt(versionIdField.getText()), targetCdmVersion.getSelectedItem().toString(), frame, workingFolderField.getText(), continueOnError);
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
						process.process(vocabFolderField.getText(), workingFolderField.getText(), vocabServerTempFolderField.getText(), vocabServerTempLocalFolderField.getText(), dbSettings, frame, workingFolderField.getText());
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
				Cdm.dropStructure(structure, dbSettings, version);
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
				String localScriptsFolder = folderExists(localScriptsFolderField.getText(), true, "Local Scripts Folder", new ArrayList<String>()) ? localScriptsFolderField.getText() : null;
				Cdm.createSchema(structure, dbSettings, version);
				Cdm.createTables(structure, dbSettings, version, localScriptsFolder, idsToBigInt, webAPIServerField.getText(), webAPIPortField.getText());
				Cdm.createPatchTables(structure, dbSettings, version, localScriptsFolder, idsToBigInt);
			} catch (Exception e) {
				handleError(e);
			} finally {
				for (JComponent component : componentsToDisableWhenRunning)
					component.setEnabled(true);
			}
		}
	}


	private class PrimaryKeysThread extends Thread {
		private int structure;

		public PrimaryKeysThread(int structure) {
			this.structure = structure;
		}

		public void run() {
			for (JComponent component : componentsToDisableWhenRunning)
				component.setEnabled(false);
			try {
				DbSettings dbSettings = getTargetDbSettings();
				String version = targetCdmVersion.getSelectedItem().toString();
				String localScriptsFolder = folderExists(localScriptsFolderField.getText(), true, "Local Scripts Folder", new ArrayList<String>()) ? localScriptsFolderField.getText() : null;
				Cdm.createPrimaryKeys(structure, dbSettings, version, localScriptsFolder);
				Cdm.createPatchPrimaryKeys(structure, dbSettings, version, localScriptsFolder);
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
				String localScriptsFolder = folderExists(localScriptsFolderField.getText(), true, "Local Scripts Folder", new ArrayList<String>()) ? localScriptsFolderField.getText() : null;
				Cdm.createIndices(structure, dbSettings, version, localScriptsFolder);
				Cdm.createPatchIndices(structure, dbSettings, version, localScriptsFolder);
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
				String localScriptsFolder = folderExists(localScriptsFolderField.getText(), true, "Local Scripts Folder", new ArrayList<String>()) ? localScriptsFolderField.getText() : null;
				Cdm.createConstraints(structure, dbSettings, version, localScriptsFolder);
				Cdm.createPatchConstraints(structure, dbSettings, version, localScriptsFolder);
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
				switch (targetCdmVersion.getSelectedItem().toString().substring(0, 1)) {
				case "5":
					version = EraBuilder.VERSION_5;
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
			String errorReportFilename = ErrorReport.generate(workingFolderField.getText(), e, null);
			String message = "Error: " + e.getLocalizedMessage();
			message += "\nAn error report has been generated:\n" + errorReportFilename;
			System.out.println(message);
			JOptionPane.showMessageDialog(frame, StringUtilities.wordWrap(message, 80), "Error", JOptionPane.ERROR_MESSAGE);
		}
	}


	private void runAll() {
		ObjectExchange.console.setDebugFile(workingFolderField.getText() + "/Console.txt");
		AutoRunThread autoRunThread = new AutoRunThread();
		autoRunThread.start();
	}
}
