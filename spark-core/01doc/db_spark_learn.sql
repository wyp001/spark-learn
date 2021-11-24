/*
Navicat MySQL Data Transfer

Source Server         : LocalMysql
Source Server Version : 50716
Source Host           : localhost:3306
Source Database       : db_spark_learn

Target Server Type    : MYSQL
Target Server Version : 50716
File Encoding         : 65001

Date: 2021-11-24 10:48:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for t_user
-- ----------------------------
DROP TABLE IF EXISTS `t_user`;
CREATE TABLE `t_user` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `age` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of t_user
-- ----------------------------
INSERT INTO `t_user` VALUES ('1', 'zhangsan', '10');
INSERT INTO `t_user` VALUES ('2', 'lisi', '20');
INSERT INTO `t_user` VALUES ('3', 'wangwu', '30');
INSERT INTO `t_user` VALUES ('4', 'zhaoliu', '40');
