{-# LANGUAGE FlexibleContexts, UndecidableInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Database.Persist.Zookeeper.Query
       where

import Database.Persist
import Data.Monoid
import Data.List (sortBy)
import Control.Monad
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Trans.Reader
import qualified Data.Text as T
import qualified Database.Zookeeper as Z
import Database.Persist.Zookeeper.Config
import Database.Persist.Zookeeper.Internal
import Database.Persist.Zookeeper.Store ()
import Database.Persist.Zookeeper.ZooUtil
import Data.Conduit
import qualified Data.Conduit.List as CL
import Data.Acquire

instance PersistQuery Z.Zookeeper where
  updateWhere filterList valList = do
    stat <- ask
    srcRes <- selectKeysRes filterList []
    liftIO $ with srcRes ( $$ loop stat)
    where
      loop stat = do
        key <- await
        case key of
          Just key' -> do
            liftIO $ flip runReaderT stat $ update key' valList
            loop stat
          Nothing ->
            return ()
  deleteWhere filterList = do
    (str::[String]) <- execZookeeper $ \zk -> do
      zGetChildren zk (filter2path filterList)
    loop str
    where
      loop [] = return ()
      loop (x:xs) = do
        let key = txtToKey x
        case filterList of
          [] -> delete key
          _ -> del key
        loop xs
      del key = do
        va <- get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then delete key
              else return ()
        
  selectSourceRes filterList opt = do
    stat <- ask
    (str::[String]) <- liftIO $ flip runReaderT stat $ do
      keys <- execZookeeper $ \zk -> do
        Z.getChildren zk (filter2path filterList) Nothing
      v <- selectOptParser keys opt
--      liftIO $ print $ v
      return v
    return $ return $ loop stat str
    where
      loop _ [] = return ()
      loop stat (x:xs) = do
        let key = txtToKey x
        va <- liftIO $ flip runReaderT stat $ get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then yield $ Entity key v
              else return ()
        loop stat xs
  selectFirst filterList opt =  do
    srcRes <- selectSourceRes filterList opt
    liftIO $ with srcRes ( $$ CL.head)
  selectKeysRes filterList opt = do 
    stat <- ask
    (str::[String]) <- liftIO $ flip runReaderT stat $ do
      keys <- execZookeeper $ \zk -> do
        Z.getChildren zk (filter2path filterList) Nothing
      selectOptParser keys opt
    return $ return (loop stat str)
    where
      loop _ [] = return ()
      loop stat (x:xs) = do
        let key = txtToKey x
        va <- liftIO $ flip runReaderT stat $ get key
        case va of
          Nothing -> return ()
          Just v -> do
            let (chk,_,_) = filterClause v filterList 
            if chk
              then yield key
              else return ()
        loop stat xs
  count filterList = do
    v <- selectList filterList []
    return $ length v

  
dummyFromFilts :: [Filter v] -> Maybe v
dummyFromFilts _ = Nothing

data OrNull = OrNullYes | OrNullNo


filterClauseHelper :: PersistEntity val
             => Bool -- ^ include WHERE?
             -> OrNull
             -> val
             -> [Filter val]
             -> (Bool, T.Text, [PersistValue])
filterClauseHelper includeWhere orNull val filters =
    (bool, if not (T.null sql) && includeWhere
            then " WHERE " <> sql
            else sql, vals)
  where
    (bool, sql, vals) = combineAND filters
    combineAND = combine " AND " (&&)
    combineOR = combine " OR " (||)

    combine s op fs =
        (foldr1 op c ,T.intercalate s $ map wrapP a, mconcat b)
      where
        (c, a, b) = unzip3 $ map go fs
        wrapP x = T.concat ["(", x, ")"]
    go (BackendFilter _) = error "BackendFilter not expected"
    go (FilterAnd []) = (True,"1=1", [])
    go (FilterAnd fs) = combineAND fs
    go (FilterOr []) = (False,"1=0", [])
    go (FilterOr fs)  = combineOR fs
    go (Filter field value pfilter) = 
      (showSqlFilter' pfilter (fieldval field val) allVals, 
      name <> ":"
      <> T.pack (show (fieldval field val)) <> ":" 
      <> showSqlFilter pfilter
      <> T.pack (show (showSqlFilter' pfilter (fieldval field val) allVals))
      <> "?5:" <> T.pack (show allVals) <> orNullSuffix, allVals) 
      where
        
        filterValueToPersistValues :: forall a.  PersistField a => Either a [a] -> [PersistValue]
        filterValueToPersistValues v = map toPersistValue $ either return id v

        orNullSuffix =
            case orNull of
                OrNullYes -> mconcat [" OR ", name, " IS NULL"]
                OrNullNo -> ""

        allVals = filterValueToPersistValues value
        name = unDBName $ fieldDB $ persistFieldDef field
        showSqlFilter Eq = "="
        showSqlFilter Ne = "<>"
        showSqlFilter Gt = ">"
        showSqlFilter Lt = "<"
        showSqlFilter Ge = ">="
        showSqlFilter Le = "<="
        showSqlFilter In = " IN "
        showSqlFilter NotIn = " NOT IN "
        showSqlFilter (BackendSpecificFilter s) = s
        showSqlFilter' :: PersistFilter -> PersistValue -> [PersistValue] -> Bool
        showSqlFilter' Eq a b = (==) a (head b)
        showSqlFilter' Ne a b = (/=) a (head b)
        showSqlFilter' Gt a b = (>)  a (head b)
        showSqlFilter' Lt a b = (<)  a (head b)
        showSqlFilter' Ge a b = (>=) a (head b)
        showSqlFilter' Le a b = (<=) a (head b)
        showSqlFilter' In _ [] = False
        showSqlFilter' In a (x:xs) = if a==x then True else showSqlFilter' In a xs
        showSqlFilter' NotIn _ [] = True
        showSqlFilter' NotIn a (x:xs) = if a==x then False else showSqlFilter' NotIn a xs
        showSqlFilter' (BackendSpecificFilter _s) _ _ =  error "not supported"


filterClause :: PersistEntity val
             => val
             -> [Filter val]
             -> (Bool, T.Text, [PersistValue])
filterClause val [] = (True,"",[])
filterClause val filter = filterClauseHelper True OrNullNo val filter

cmp' (k0,v0) (k1,v1) = compare v0 v1
cmp'' (k0,v0) (k1,v1) = compare v1 v0


conva ::  [[String]] -> [(String,Int)]
conva keys = concat $ map (\(i,ks) -> map ((,) i) ks) $ zip [0..] keys
convb ::  [(String,Int)] -> [[String]]
convb keys = concat $ map (\(i,ks) -> map ((,) i) ks) $ zip [0..] keys

selectOptParser' :: (PersistStore backend, MonadIO m, PersistEntity val, backend ~ PersistEntityBackend val) 
                 => [[String]]
                 -> [SelectOpt val]
                 -> ReaderT backend m [[String]]
selectOptParser' keys [] = do
  return keys
selectOptParser' keys (OffsetBy i:xs) = do
  selectOptParser (drop i keys) xs
selectOptParser' keys (LimitTo i:xs) = do
  selectOptParser (take i keys) xs
selectOptParser' keys (Asc field:xs) = do
  keysWithVal <- forM keys $ \x -> do 
    let key = txtToKey x
    val <- get key
    case val of
      Nothing -> fail "can not get value"
      Just v -> return $ (x,fieldval field v)
  selectOptParser (map fst $ sortBy cmp' keysWithVal) xs
selectOptParser' keys (Desc field:xs) = do
  keysWithVal <- forM keys $ \x -> do 
    let key = txtToKey x
    val <- get key
    case val of
      Nothing -> fail "can not get value"
      Just v -> return $ (x,fieldval field v)
  selectOptParser (map fst $ sortBy cmp'' keysWithVal) xs

selectOptParser keys opt = do
  keys' <- selectOptParser' [keys] opt
  return $ concat keys'
