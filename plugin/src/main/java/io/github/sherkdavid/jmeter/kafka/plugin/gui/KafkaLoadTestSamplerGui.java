/**
 * Copyright 2026 David Murphy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.sherkdavid.jmeter.kafka.plugin.gui;

import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import io.github.sherkdavid.jmeter.kafka.plugin.sampler.KafkaLoadTestSampler;

import javax.swing.*;
import java.awt.*;

/**
 * GUI for the Kafka Load Test Sampler.
 * Provides UI for configuring Kafka connection and message producer settings.
 */
public class KafkaLoadTestSamplerGui extends AbstractSamplerGui {
    
    private static final long serialVersionUID = 1L;
    
    private JTextField bootstrapServersField;
    private JTextField topicField;
    private JTextField producerClassField;
    private JTextField messageKeyField;
    private JTextField batchSizeField;
    private JTextField lingerMsField;
    private JComboBox<String> acksCombo;
    private JComboBox<String> compressionTypeCombo;
    
    /**
     * Constructor for KafkaLoadTestSamplerGui
     */
    public KafkaLoadTestSamplerGui() {
        super();
        init();
    }
    
    /**
     * Initialize the GUI components
     */
    private void init() {
        setLayout(new BorderLayout());
        setBorder(makeBorder());
        
        add(makeTitlePanel(), BorderLayout.NORTH);
        add(createMainPanel(), BorderLayout.CENTER);
    }
    
    /**
     * Create the main configuration panel
     */
    private JPanel createMainPanel() {
        JPanel mainPanel = new JPanel();
        mainPanel.setLayout(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(5, 5, 5, 5);
        gbc.fill = GridBagConstraints.HORIZONTAL;
        
        // Kafka Connection Settings
        mainPanel.add(new JLabel("Kafka Connection:"), createHeaderConstraints(gbc, 0));
        
        gbc.gridy = 1;
        mainPanel.add(new JLabel("Bootstrap Servers:"), createLabelConstraints(gbc));
        bootstrapServersField = new JTextField(20);
        mainPanel.add(bootstrapServersField, createFieldConstraints(gbc));
        
        gbc.gridy = 2;
        mainPanel.add(new JLabel("Topic:"), createLabelConstraints(gbc));
        topicField = new JTextField(20);
        mainPanel.add(topicField, createFieldConstraints(gbc));
        
        // Message Producer Settings
        gbc.gridy = 3;
        mainPanel.add(new JLabel("Message Producer:"), createHeaderConstraints(gbc, 3));
        
        gbc.gridy = 4;
        mainPanel.add(new JLabel("Producer Class:"), createLabelConstraints(gbc));
        producerClassField = new JTextField(30);
        mainPanel.add(producerClassField, createFieldConstraints(gbc));
        
        // Optional Settings
        gbc.gridy = 5;
        mainPanel.add(new JLabel("Optional Settings:"), createHeaderConstraints(gbc, 5));
        
        gbc.gridy = 6;
        mainPanel.add(new JLabel("Message Key:"), createLabelConstraints(gbc));
        messageKeyField = new JTextField(20);
        mainPanel.add(messageKeyField, createFieldConstraints(gbc));
        
        // Producer Configuration
        gbc.gridy = 7;
        mainPanel.add(new JLabel("Producer Configuration:"), createHeaderConstraints(gbc, 7));
        
        gbc.gridy = 8;
        mainPanel.add(new JLabel("Batch Size:"), createLabelConstraints(gbc));
        batchSizeField = new JTextField("16384", 10);
        mainPanel.add(batchSizeField, createFieldConstraints(gbc));
        
        gbc.gridy = 9;
        mainPanel.add(new JLabel("Linger (ms):"), createLabelConstraints(gbc));
        lingerMsField = new JTextField("10", 10);
        mainPanel.add(lingerMsField, createFieldConstraints(gbc));
        
        gbc.gridy = 10;
        mainPanel.add(new JLabel("Acks:"), createLabelConstraints(gbc));
        acksCombo = new JComboBox<>(new String[]{"0", "1", "all"});
        mainPanel.add(acksCombo, createFieldConstraints(gbc));
        
        gbc.gridy = 11;
        mainPanel.add(new JLabel("Compression:"), createLabelConstraints(gbc));
        compressionTypeCombo = new JComboBox<>(new String[]{"none", "gzip", "snappy", "lz4", "zstd"});
        mainPanel.add(compressionTypeCombo, createFieldConstraints(gbc));
        
        // Add filler panel
        gbc.gridy = 12;
        gbc.weighty = 1.0;
        mainPanel.add(new JPanel(), createFieldConstraints(gbc));
        
        JScrollPane scrollPane = new JScrollPane(mainPanel);
        JPanel wrapper = new JPanel(new BorderLayout());
        wrapper.add(scrollPane, BorderLayout.CENTER);
        
        return wrapper;
    }
    
    /**
     * Helper method to create header constraints
     */
    private GridBagConstraints createHeaderConstraints(GridBagConstraints gbc, int row) {
        GridBagConstraints result = (GridBagConstraints) gbc.clone();
        result.gridx = 0;
        result.gridy = row;
        result.gridwidth = 2;
        result.insets = new Insets(10, 5, 5, 5);
        return result;
    }
    
    /**
     * Helper method to create label constraints
     */
    private GridBagConstraints createLabelConstraints(GridBagConstraints gbc) {
        GridBagConstraints result = (GridBagConstraints) gbc.clone();
        result.gridx = 0;
        result.gridwidth = 1;
        result.weightx = 0.0;
        return result;
    }
    
    /**
     * Helper method to create field constraints
     */
    private GridBagConstraints createFieldConstraints(GridBagConstraints gbc) {
        GridBagConstraints result = (GridBagConstraints) gbc.clone();
        result.gridx = 1;
        result.gridwidth = 1;
        result.weightx = 1.0;
        return result;
    }
    
    @Override
    public String getStaticLabel() {
        return "Kafka Load Test Sampler";
    }
    
    @Override
    public String getLabelResource() {
        return "kafka_load_test_sampler";
    }
    
    @Override
    public TestElement createTestElement() {
        KafkaLoadTestSampler sampler = new KafkaLoadTestSampler();
        modifyTestElement(sampler);
        return sampler;
    }
    
    @Override
    public void modifyTestElement(TestElement element) {
        if (element instanceof KafkaLoadTestSampler) {
            KafkaLoadTestSampler sampler = (KafkaLoadTestSampler) element;
            sampler.setBootstrapServers(bootstrapServersField.getText());
            sampler.setTopic(topicField.getText());
            sampler.setProducerClass(producerClassField.getText());
            sampler.setMessageKey(messageKeyField.getText());
            sampler.setBatchSize(batchSizeField.getText());
            sampler.setLingerMs(lingerMsField.getText());
            sampler.setAcks((String) acksCombo.getSelectedItem());
            sampler.setCompressionType((String) compressionTypeCombo.getSelectedItem());
        }
    }
    
    @Override
    public void configure(TestElement element) {
        super.configure(element);
        
        if (element instanceof KafkaLoadTestSampler) {
            KafkaLoadTestSampler sampler = (KafkaLoadTestSampler) element;
            bootstrapServersField.setText(sampler.getBootstrapServers());
            topicField.setText(sampler.getTopic());
            producerClassField.setText(sampler.getProducerClass());
            messageKeyField.setText(sampler.getMessageKey());
            batchSizeField.setText(sampler.getBatchSize());
            lingerMsField.setText(sampler.getLingerMs());
            acksCombo.setSelectedItem(sampler.getAcks());
            compressionTypeCombo.setSelectedItem(sampler.getCompressionType());
        }
    }
    
    @Override
    public void clearGui() {
        super.clearGui();
        bootstrapServersField.setText("localhost:9092");
        topicField.setText("test-topic");
        producerClassField.setText("");
        messageKeyField.setText("");
        batchSizeField.setText("16384");
        lingerMsField.setText("10");
        acksCombo.setSelectedIndex(1);
        compressionTypeCombo.setSelectedIndex(0);
    }
}
