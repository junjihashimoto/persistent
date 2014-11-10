{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.ZooUtil
    where

import qualified Database.Zookeeper as Z
import qualified Data.ByteString.Char8 as B
import Control.Monad
import Data.Monoid

deriving instance Read (Z.ZKError)
deriving instance Read (Z.Stat)

zModify :: Z.Zookeeper
       -> String
       -> (Maybe B.ByteString -> IO (Maybe B.ByteString))
       -> IO (Either Z.ZKError Z.Stat)
zModify  zk key f = do
  v <- Z.get zk key Nothing
  case v of
    Right (con,ver) -> do
      v'' <- f con
      v' <- Z.set zk key v'' (Just (Z.statVersion ver))
      case v' of
        Right _ -> return v'
        Left _ -> zModify zk key f
    Left e -> return $ Left e

zReplace :: Z.Zookeeper
       -> String
       -> (Maybe B.ByteString)
       -> IO (Either Z.ZKError Z.Stat)
zReplace  zk key v'' = do
  v <- Z.get zk key Nothing
  case v of
    Right (_con,ver) -> do
      v' <- Z.set zk key v'' (Just (Z.statVersion ver))
      case v' of
        Right _ -> return v'
        Left _ -> zReplace zk key v''
    Left e -> return $ Left e

zRepSert :: Z.Zookeeper
       -> String
       -> String
       -> (Maybe B.ByteString)
       -> IO (Either Z.ZKError ())
zRepSert  zk dir key v'' = do
  v <- zCreate zk dir key v'' []
  case v of
    Right _ -> return $ Right ()
    Left Z.NodeExistsError -> do
      v' <- zReplace zk key v''
      case v' of
        Right _ -> return $ Right ()
        Left Z.NoNodeError -> do
          zRepSert zk dir key v''
        Left s -> do
          return $ Left s
    Left v' -> return $ Left v'

zCreate :: Z.Zookeeper
       -> String
       -> String
       -> Maybe B.ByteString
       -> [Z.CreateFlag]
       -> IO (Either Z.ZKError String)
zCreate zk dir path value flag = do
  v <- Z.create zk path value Z.OpenAclUnsafe flag
  case v of
    Left Z.NoNodeError -> do
      v' <- Z.create zk dir Nothing Z.OpenAclUnsafe []
      case v' of
        Left v'' -> return $ Left v''
        Right _ -> zCreate zk dir path value flag
    v' -> return v'

zDeleteRecursive :: Z.Zookeeper
                 -> String
                 -> IO (Either Z.ZKError ())
zDeleteRecursive zk dir = do
  ls <- zGetTree zk dir
  res <- forM (reverse ls) $ \node -> 
    Z.delete zk node Nothing
  return $ checkRes res
  where
    checkRes [] = Right ()
    checkRes (Left val:_) = Left val
    checkRes (Right _:xs) = checkRes xs
  

zGetTree :: Z.Zookeeper
         -> String
         -> IO [String]
zGetTree zk dir = do
  ls <- Z.getChildren zk dir Nothing
  case ls of
    Right dir' -> do
      ls' <- forM dir' $ \d -> do 
        zGetTree zk (dir <> "/" <> d)
      return $ flatten ls' 
    Left err' -> error ("zGetTree's error:" ++ show err')
  where
    flatten [[a]] = [a]
    flatten [] = []
    flatten (x:xs) = x ++ flatten xs
