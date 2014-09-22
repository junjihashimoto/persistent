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
-- import qualified Data.ByteString.Lazy as BL
import qualified Data.Pool as P
import Control.Concurrent
import Control.Concurrent.STM
import Data.Time
import Data.Maybe

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

data ZooStat = ZooStat {
    zVar :: TMVar (ZAction String)
  , rVar :: TMVar String
  }

data ZAction t = 
    Action (Z.Zookeeper -> IO String)
  | Close

zOpen ::   String
        -> Z.Timeout
        -> Maybe Z.Watcher
        -> Maybe Z.ClientID
        -> IO ZooStat
zOpen endpoint timeout watcher clientId = do
  Z.setDebugLevel Z.ZLogError
  zvar <- atomically $ newEmptyTMVar
  rvar <- atomically $ newEmptyTMVar
  stateVar <- newTVarIO Z.ConnectingState
  let watcher' = if isJust watcher then watcher else (Just (watcherd stateVar))
  _ <- forkIO $ Z.withZookeeper endpoint timeout watcher' clientId $ \z -> do
    atomically $ do
      state <- readTVar stateVar
      case state of
        Z.ConnectingState -> retry
        _ -> return ()
    loop zvar rvar z
  return $ ZooStat zvar rvar
  where
    loop zvar rvar z = do
      var <- atomically $ takeTMVar zvar
      case var of
        Close -> return ()
        Action io -> do
          v <- io z
          atomically $ putTMVar rvar v
          loop zvar rvar z
    watcherd :: TVar Z.State -> Z.Watcher
    watcherd stateVar _z event state _mZnode = do
      case event of
        Z.SessionEvent -> atomically $ writeTVar stateVar state
        _ -> return ()

zExec :: (Read a, Show a) => ZooStat -> (Z.Zookeeper -> IO a) -> IO a
zExec (ZooStat zvar rvar) action = do
  atomically $ do
    putTMVar zvar $ Action $ \zk -> do
      v <- action zk
      return $ show v
  var <- atomically $ do
    takeTMVar rvar
  return $ read var

zClose :: ZooStat -> IO ()
zClose (ZooStat zvar _rvar) = do
  atomically $ do
    putTMVar zvar Close

connect :: String
        -> Z.Timeout
        -> Maybe Z.Watcher
        -> Maybe Z.ClientID
        -> Int
        -> NominalDiffTime
        -> Int
        -> IO (P.Pool ZooStat)
connect endpoint timeout watcher clientId numStripes idleTime maxResources =
  P.createPool
    (zOpen endpoint timeout watcher clientId) 
    zClose
    numStripes idleTime maxResources
